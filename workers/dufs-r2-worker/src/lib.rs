//! A Cloudflare Workers/R2 deployment target for Dufs.
//!
//! Cloudflare Workers do not expose a POSIX filesystem or allow a process to bind a socket, so
//! this module intentionally implements the WebDAV surface against an R2 bucket instead of trying
//! to compile the native Tokio server unchanged.

use base64::{
    engine::general_purpose::{STANDARD as BASE64, URL_SAFE_NO_PAD as BASE64_URL},
    Engine as _,
};
use hmac::{Hmac, Mac};
use percent_encoding::{percent_decode_str, utf8_percent_encode, NON_ALPHANUMERIC};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sha2::Sha256;
use std::collections::HashMap;
use worker::{
    durable_object, event, Bucket, Context, DurableObject, Env, FixedLengthStream, Headers,
    HttpMetadata, Object, Request, Response, Result, State,
};

// Bump this path whenever embedded browser assets change. Assets are served
// immutable for one year, so the versioned path safely bypasses old UI caches.
const ASSET_PREFIX: &str = "__dufs_wasm_v7__/";
const DIRECTORY_MARKER: &str = ".dufs-directory";
const PUBLIC_SHARE_ROOT: &str = "share";
const EDITABLE_TEXT_MAX_SIZE: u64 = 4 * 1024 * 1024;
const MAX_LIST_RESULTS: u32 = 1000;
const MAX_SEARCH_SCAN: usize = 5000;
const MAX_PATCH_BYTES: u64 = 10 * 1024 * 1024;
const TOKEN_TTL_MILLIS: u64 = 3 * 24 * 60 * 60 * 1000;
const MULTIPART_PART_SIZE: u64 = 16 * 1024 * 1024;
const MAX_MULTIPART_PARTS: u64 = 10_000;
const MULTIPART_SESSION_TTL_MILLIS: u64 = 24 * 60 * 60 * 1000;

const INDEX_HTML: &str = include_str!("../../../assets/index.html");
const INDEX_CSS: &str = include_str!("../../../assets/index.css");
const INDEX_JS: &str = include_str!("../../../assets/index.js");
const FAVICON_ICO: &[u8] = include_bytes!("../../../assets/favicon.ico");

type HmacSha256 = Hmac<Sha256>;

#[derive(Serialize)]
struct IndexData {
    href: String,
    kind: DataKind,
    uri_prefix: String,
    allow_upload: bool,
    allow_delete: bool,
    allow_search: bool,
    allow_archive: bool,
    dir_exists: bool,
    auth: bool,
    user: Option<String>,
    paths: Vec<PathItem>,
}

#[derive(Serialize)]
struct EditData {
    href: String,
    kind: DataKind,
    uri_prefix: String,
    allow_upload: bool,
    allow_delete: bool,
    auth: bool,
    user: Option<String>,
    editable: bool,
}

#[derive(Deserialize)]
struct MultipartStartRequest {
    size: u64,
    content_type: Option<String>,
}

#[derive(Serialize)]
struct MultipartStartResponse {
    session: String,
    part_size: u64,
}

#[derive(Serialize)]
struct MultipartPartResponse {
    part_number: u16,
    etag: String,
}

#[derive(Deserialize)]
struct MultipartCompleteRequest {
    session: String,
    parts: Vec<MultipartClientPart>,
}

#[derive(Deserialize)]
struct MultipartClientPart {
    part_number: u16,
    etag: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct MultipartSession {
    key: String,
    upload_id: String,
    size: u64,
    expires_at: u64,
}

#[derive(Serialize)]
enum DataKind {
    Index,
    Edit,
    View,
}

#[derive(Serialize, Clone)]
struct PathItem {
    path_type: PathType,
    name: String,
    mtime: u64,
    size: u64,
}

#[derive(Serialize, Clone, Copy, Eq, PartialEq)]
enum PathType {
    Dir,
    File,
}

impl PathItem {
    fn is_dir(&self) -> bool {
        self.path_type == PathType::Dir
    }
}

enum Node {
    File(Object),
    Dir,
    Missing,
}

// Each multipart upload is routed to one Durable Object. This moves the
// Workers-RS stream bridge (which copies chunks through WASM) out of the
// Free Worker 10 ms CPU budget and also serializes parts for a given upload.
#[durable_object(fetch)]
pub struct MultipartUploadPart {
    env: Env,
}

impl DurableObject for MultipartUploadPart {
    fn new(_state: State, env: Env) -> Self {
        Self { env }
    }

    async fn fetch(&self, mut req: Request) -> Result<Response> {
        let key = match normalize_key(&req.path()) {
            Ok(key) => key,
            Err(()) => return text_response(400, "Invalid Path"),
        };
        let query = query_params(&req)?;
        if req.inner().method() != "PUT"
            || !query
                .get("__dufs_multipart")
                .is_some_and(|action| action == "part")
        {
            return text_response(405, "Invalid multipart upload operation");
        }
        let bucket = self.env.bucket("DUFS_BUCKET")?;
        handle_multipart_part(&bucket, &key, &mut req, &self.env, &query).await
    }
}

// Standard PUT/PATCH uploads also pass through the worker-rs stream bridge.
// Route each object key to a Durable Object so WebDAV uploads and small browser
// PUT uploads do not consume the entry Worker's small CPU budget.
#[durable_object(fetch)]
pub struct ObjectUpload {
    env: Env,
}

impl DurableObject for ObjectUpload {
    fn new(_state: State, env: Env) -> Self {
        Self { env }
    }

    async fn fetch(&self, mut req: Request) -> Result<Response> {
        let method = req.inner().method().to_ascii_uppercase();
        if !matches!(method.as_str(), "PUT" | "PATCH") {
            return text_response(405, "Invalid object upload operation");
        }
        let key = match normalize_key(&req.path()) {
            Ok(key) => key,
            Err(()) => return text_response(400, "Invalid Path"),
        };
        let bucket = self.env.bucket("DUFS_BUCKET")?;
        match method.as_str() {
            "PUT" => handle_put(&bucket, &key, &mut req).await,
            "PATCH" => handle_patch(&bucket, &key, &mut req).await,
            _ => text_response(405, "Invalid object upload operation"),
        }
    }
}

// R2 copies pass through the worker-rs stream bridge, which copies each chunk
// through WASM. Route each source key to a dedicated Durable Object so a large
// WebDAV COPY/MOVE does not consume the entry Worker's small CPU budget.
#[durable_object(fetch)]
pub struct ObjectCopy {
    env: Env,
}

impl DurableObject for ObjectCopy {
    fn new(_state: State, env: Env) -> Self {
        Self { env }
    }

    async fn fetch(&self, req: Request) -> Result<Response> {
        let method = req.inner().method().to_ascii_uppercase();
        if !matches!(method.as_str(), "COPY" | "MOVE") {
            return text_response(405, "Invalid object copy operation");
        }
        let key = match normalize_key(&req.path()) {
            Ok(key) => key,
            Err(()) => return text_response(400, "Invalid Path"),
        };
        let bucket = self.env.bucket("DUFS_BUCKET")?;
        handle_copy_or_move(&bucket, &key, &req, method == "MOVE").await
    }
}

#[event(fetch)]
async fn fetch(req: Request, env: Env, _ctx: Context) -> Result<Response> {
    match handle_request(req, &env).await {
        Ok(response) => Ok(response),
        Err(error) => {
            worker::console_error!("dufs-r2-worker request failed: {error}");
            text_response(500, "Internal Server Error")
        }
    }
}

async fn handle_request(mut req: Request, env: &Env) -> Result<Response> {
    if let Some(response) = internal_response(&req)? {
        return Ok(response);
    }

    let key = match normalize_key(&req.path()) {
        Ok(key) => key,
        Err(()) => return text_response(400, "Invalid Path"),
    };
    let query = query_params(&req)?;
    let method = req.inner().method().to_ascii_uppercase();

    // `/share/` without credentials is a strictly read-only, prefix-scoped
    // R2 view. Requests that carry Basic Auth continue through the normal
    // WebDAV flow, so administrators can still manage the share directory.
    if is_public_share_key(&key) && req.headers().get("authorization")?.is_none() {
        let bucket = env.bucket("DUFS_BUCKET")?;
        return handle_public_share(&bucket, &key, &req, &method).await;
    }

    let authenticated_user = match authenticate(&req, env, &key, &method, &query)? {
        Some(user) => user,
        None => return unauthorized_response(),
    };
    let read_only = env
        .var("DUFS_READ_ONLY")
        .ok()
        .map(|value| value.to_string().eq_ignore_ascii_case("true"))
        .unwrap_or(false);

    // Cloudflare Workers returns 501 for arbitrary methods before invoking the
    // Worker. The built-in UI therefore uses standard POST compatibility
    // endpoints instead of the native Dufs CHECKAUTH/LOGOUT methods.
    if (method == "POST" && query.contains_key("__dufs_checkauth")) || method == "CHECKAUTH" {
        return Response::ok(authenticated_user);
    }
    if (method == "POST" && query.contains_key("__dufs_logout")) || method == "LOGOUT" {
        return unauthorized_response();
    }
    if query.contains_key("tokengen") {
        if method != "GET" && method != "HEAD" {
            return text_response(405, "Method Not Allowed");
        }
        return token_response(env, &key);
    }

    let bucket = env.bucket("DUFS_BUCKET")?;
    if let Some(action) = query.get("__dufs_multipart") {
        if read_only {
            return text_response(403, "Read-only");
        }
        return handle_multipart_action(&bucket, &key, req, env, &method, action, &query).await;
    }
    match method.as_str() {
        "GET" | "HEAD" => {
            let head_only = method == "HEAD";
            handle_read(
                &bucket,
                &key,
                &mut req,
                &query,
                &authenticated_user,
                read_only,
                head_only,
            )
            .await
        }
        "OPTIONS" => options_response(),
        "PUT" => {
            if read_only {
                return text_response(403, "Read-only");
            }
            handle_upload_via_durable_object(&key, req, env).await
        }
        "PATCH" => {
            if read_only {
                return text_response(403, "Read-only");
            }
            handle_upload_via_durable_object(&key, req, env).await
        }
        "DELETE" => {
            if read_only {
                return text_response(403, "Read-only");
            }
            handle_delete(&bucket, &key).await
        }
        "PROPFIND" => handle_propfind(&bucket, &key, &mut req).await,
        "PROPPATCH" => proppatch_response(&key),
        "MKCOL" => {
            if read_only {
                return text_response(403, "Read-only");
            }
            handle_mkcol(&bucket, &key).await
        }
        "COPY" => {
            if read_only {
                return text_response(403, "Read-only");
            }
            handle_copy_or_move_via_durable_object(&key, req, env).await
        }
        "MOVE" => {
            if read_only {
                return text_response(403, "Read-only");
            }
            handle_copy_or_move_via_durable_object(&key, req, env).await
        }
        "LOCK" => handle_lock(&key),
        "UNLOCK" => Ok(Response::builder().with_status(204).empty()),
        _ => text_response(405, "Method Not Allowed"),
    }
}

async fn handle_copy_or_move_via_durable_object(
    key: &str,
    req: Request,
    env: &Env,
) -> Result<Response> {
    if key.is_empty() {
        return text_response(403, "Cannot copy or move the root collection");
    }
    // A source key is the coordination atom: overlapping moves for one source
    // are serialized, while unrelated files remain independent.
    let namespace = env.durable_object("OBJECT_COPY")?;
    let stub = namespace.get_by_name(key)?;
    stub.fetch_with_request(req).await
}

async fn handle_upload_via_durable_object(key: &str, req: Request, env: &Env) -> Result<Response> {
    let namespace = env.durable_object("OBJECT_UPLOAD")?;
    let object_name = if key.is_empty() { "/" } else { key };
    let stub = namespace.get_by_name(object_name)?;
    stub.fetch_with_request(req).await
}

fn internal_response(req: &Request) -> Result<Option<Response>> {
    let path = req.path();
    if path == "/__dufs__/health" {
        return Ok(Some(
            Response::builder()
                .with_header("content-type", "application/json")?
                .fixed(br#"{"status":"OK"}"#.to_vec()),
        ));
    }
    if req.inner().method() != "GET" {
        return Ok(None);
    }
    let asset_name = match path.trim_start_matches('/').strip_prefix(ASSET_PREFIX) {
        Some(name) => name,
        None => return Ok(None),
    };
    let response = match asset_name {
        "index.js" => Response::builder()
            .with_header("content-type", "application/javascript; charset=UTF-8")?
            .with_header("cache-control", "public, max-age=31536000, immutable")?
            .with_header("x-content-type-options", "nosniff")?
            .fixed(INDEX_JS.as_bytes().to_vec()),
        "index.css" => Response::builder()
            .with_header("content-type", "text/css; charset=UTF-8")?
            .with_header("cache-control", "public, max-age=31536000, immutable")?
            .with_header("x-content-type-options", "nosniff")?
            .fixed(INDEX_CSS.as_bytes().to_vec()),
        "favicon.ico" => Response::builder()
            .with_header("content-type", "image/x-icon")?
            .with_header("cache-control", "public, max-age=31536000, immutable")?
            .with_header("x-content-type-options", "nosniff")?
            .fixed(FAVICON_ICO.to_vec()),
        _ => return Ok(Some(text_response(404, "Not Found")?)),
    };
    Ok(Some(response))
}

fn authenticate(
    req: &Request,
    env: &Env,
    key: &str,
    method: &str,
    query: &HashMap<String, String>,
) -> Result<Option<String>> {
    let username = env.secret("DUFS_USERNAME")?.to_string();
    let password = env.secret("DUFS_PASSWORD")?.to_string();

    if matches!(method, "GET" | "HEAD")
        && query
            .get("token")
            .is_some_and(|token| verify_download_token(token, key, &username, &password))
    {
        return Ok(Some(username));
    }

    let Some(value) = req.headers().get("authorization")? else {
        return Ok(None);
    };
    let Some(encoded) = value.strip_prefix("Basic ") else {
        return Ok(None);
    };
    let Ok(decoded) = BASE64.decode(encoded) else {
        return Ok(None);
    };
    let Ok(decoded) = std::str::from_utf8(&decoded) else {
        return Ok(None);
    };
    let Some((provided_user, provided_password)) = decoded.split_once(':') else {
        return Ok(None);
    };
    if constant_time_eq(provided_user.as_bytes(), username.as_bytes())
        && constant_time_eq(provided_password.as_bytes(), password.as_bytes())
    {
        Ok(Some(username))
    } else {
        Ok(None)
    }
}

fn token_response(env: &Env, key: &str) -> Result<Response> {
    let username = env.secret("DUFS_USERNAME")?.to_string();
    let password = env.secret("DUFS_PASSWORD")?.to_string();
    let expiry = js_sys::Date::now() as u64 + TOKEN_TTL_MILLIS;
    let signature = token_signature(key, expiry, &username, &password);
    Ok(Response::builder()
        .with_header("content-type", "text/plain; charset=utf-8")?
        .fixed(format!("{expiry}.{signature}").into_bytes()))
}

async fn handle_read(
    bucket: &Bucket,
    key: &str,
    req: &Request,
    query: &HashMap<String, String>,
    user: &str,
    read_only: bool,
    head_only: bool,
) -> Result<Response> {
    match node_type(bucket, key).await? {
        Node::File(object) => {
            if query.contains_key("edit") || query.contains_key("view") {
                return edit_page(key, &object, user, read_only, query.contains_key("view"));
            }
            if query.contains_key("json") {
                return json_response(&file_item(key, &object));
            }
            if query.contains_key("hash") {
                return text_response(501, "?hash is not available in the Worker target");
            }
            file_response(bucket, key, &object, req, head_only, false).await
        }
        Node::Dir => {
            if query.contains_key("zip") {
                return text_response(
                    501,
                    "Directory archives are not available in the Worker target",
                );
            }
            let paths = if query.contains_key("q") {
                search_dir(
                    bucket,
                    key,
                    query.get("q").map(String::as_str).unwrap_or_default(),
                )
                .await?
            } else {
                list_dir(bucket, key).await?
            };
            index_page(key, paths, true, query, user, read_only, head_only)
        }
        Node::Missing => text_response(404, "Not Found"),
    }
}

fn is_public_share_key(key: &str) -> bool {
    key == PUBLIC_SHARE_ROOT || key.starts_with(&format!("{PUBLIC_SHARE_ROOT}/"))
}

async fn handle_public_share(
    bucket: &Bucket,
    key: &str,
    req: &Request,
    method: &str,
) -> Result<Response> {
    if !matches!(method, "GET" | "HEAD") {
        return public_share_method_not_allowed();
    }

    let head_only = method == "HEAD";
    // The share root exists as a logical directory even before the first file
    // is uploaded. A physical R2 object named `share` is intentionally not
    // exposed because the reserved public namespace is `share/`.
    let node = if key == PUBLIC_SHARE_ROOT {
        Node::Dir
    } else {
        node_type(bucket, key).await?
    };

    match node {
        Node::File(object) => file_response(bucket, key, &object, req, head_only, true).await,
        Node::Dir => {
            if !req.path().ends_with('/') {
                return public_share_redirect(&href_for_key(key, true));
            }
            public_share_index(key, list_dir(bucket, key).await?, head_only)
        }
        Node::Missing => text_response(404, "Not Found"),
    }
}

fn public_share_method_not_allowed() -> Result<Response> {
    Ok(Response::builder()
        .with_status(405)
        .with_header("allow", "GET, HEAD")?
        .with_header("content-type", "text/plain; charset=utf-8")?
        .fixed(b"Public shares are read-only".to_vec()))
}

fn public_share_redirect(location: &str) -> Result<Response> {
    Ok(Response::builder()
        .with_status(308)
        .with_header("location", location)?
        .with_header("cache-control", "no-cache")?
        .empty())
}

fn public_share_index(key: &str, mut paths: Vec<PathItem>, head_only: bool) -> Result<Response> {
    paths.sort_by_key(|item| (item.path_type as u8, item.name.to_lowercase()));

    let mut rows = String::new();
    if key != PUBLIC_SHARE_ROOT {
        let parent = key
            .rsplit_once('/')
            .map(|(parent, _)| parent)
            .unwrap_or(PUBLIC_SHARE_ROOT);
        rows.push_str(&format!(
            r#"<li class="parent"><a class="name" href="{}">../</a><span class="meta">parent folder</span></li>"#,
            html_escape(&href_for_key(parent, true))
        ));
    }
    for item in paths {
        rows.push_str(&public_share_item_row(key, &item));
    }
    if rows.is_empty() {
        rows.push_str(r#"<li class="empty"><span>This folder is empty.</span></li>"#);
    }

    let location = html_escape(&href_for_key(key, true));
    let body = format!(
        r#"<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Public share</title><style>
body{{font:16px system-ui,sans-serif;max-width:860px;margin:3rem auto;padding:0 1rem;color:#1f2937}}
h1{{font-size:1.5rem;margin-bottom:.25rem}}p{{color:#4b5563}}ul{{list-style:none;padding:0;border-top:1px solid #e5e7eb}}
li{{display:grid;grid-template-columns:minmax(0,1fr) auto auto;gap:.75rem;align-items:center;padding:.7rem .25rem;border-bottom:1px solid #e5e7eb}}
li.parent{{grid-template-columns:minmax(0,1fr) auto}}.name{{min-width:0;overflow-wrap:anywhere}}
a{{color:#2563eb;text-decoration:none}}a:hover{{text-decoration:underline}}.meta{{color:#6b7280;white-space:nowrap}}
.download{{border:1px solid #cbd5e1;border-radius:6px;padding:.25rem .55rem;white-space:nowrap}}.download:hover{{background:#eff6ff;text-decoration:none}}
@media (max-width:520px){{li{{grid-template-columns:minmax(0,1fr) auto;gap:.35rem .75rem}}.meta{{grid-column:1;grid-row:2}}.download{{grid-column:2;grid-row:1 / span 2}}li.parent .meta{{grid-column:2;grid-row:1}}}}
</style></head><body><main><h1>Public share</h1><p>Read-only files in <code>{location}</code></p><ul>{rows}</ul></main></body></html>"#
    );
    let body = body.into_bytes();
    let builder = Response::builder()
        .with_status(200)
        .with_header("content-type", "text/html; charset=utf-8")?
        .with_header("content-length", &body.len().to_string())?
        .with_header("cache-control", "no-cache")?
        .with_header(
            "content-security-policy",
            "default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'; frame-ancestors 'none'",
        )?
        .with_header("referrer-policy", "no-referrer")?
        .with_header("x-content-type-options", "nosniff")?;
    if head_only {
        Ok(builder.empty())
    } else {
        Ok(builder.fixed(body))
    }
}

fn public_share_item_row(parent_key: &str, item: &PathItem) -> String {
    let child_key = join_key(parent_key, &item.name);
    let href = html_escape(&href_for_key(&child_key, item.is_dir()));
    let label = if item.is_dir() {
        format!("{}/", html_escape(&item.name))
    } else {
        html_escape(&item.name)
    };

    if item.is_dir() {
        format!(
            r#"<li><a class="name" href="{href}">{label}</a><span class="meta">folder</span></li>"#
        )
    } else {
        let download_label = html_escape(&format!("Download {}", item.name));
        format!(
            r#"<li><a class="name" href="{href}">{label}</a><span class="meta">{} bytes</span><a class="download" href="{href}" download aria-label="{download_label}">Download</a></li>"#,
            item.size
        )
    }
}

async fn handle_put(bucket: &Bucket, key: &str, req: &mut Request) -> Result<Response> {
    if key.is_empty() {
        return text_response(405, "Cannot replace the root collection");
    }
    let existing = node_type(bucket, key).await.map_err(|error| {
        worker::console_error!("PUT could not inspect {key:?}: {error}");
        error
    })?;
    if matches!(existing, Node::Dir) {
        return text_response(405, "A collection already exists at this path");
    }
    let existed = bucket.head(key).await?.is_some();
    let metadata = upload_metadata(req)?;
    put_request_body(bucket, key, req, metadata).await?;
    Ok(Response::builder()
        .with_status(if existed { 204 } else { 201 })
        .empty())
}

async fn handle_multipart_action(
    bucket: &Bucket,
    key: &str,
    mut req: Request,
    env: &Env,
    method: &str,
    action: &str,
    query: &HashMap<String, String>,
) -> Result<Response> {
    match (method, action) {
        ("POST", "start") => handle_multipart_start(bucket, key, &mut req, env).await,
        ("PUT", "part") => handle_multipart_part_via_durable_object(key, req, env, query).await,
        ("POST", "complete") => handle_multipart_complete(bucket, key, &mut req, env).await,
        ("POST", "abort") => handle_multipart_abort(bucket, key, env, query).await,
        _ => text_response(405, "Invalid multipart upload operation"),
    }
}

async fn handle_multipart_part_via_durable_object(
    key: &str,
    req: Request,
    env: &Env,
    query: &HashMap<String, String>,
) -> Result<Response> {
    let Some(session) = multipart_session_from_query(query, env)? else {
        return text_response(403, "Invalid or expired multipart upload session");
    };
    if session.key != key {
        return text_response(403, "Multipart upload session does not match this path");
    }

    // The session's R2 upload ID names one coordination atom, so independent
    // uploads do not contend with one another while the same upload remains
    // ordered and isolated.
    let namespace = env.durable_object("MULTIPART_UPLOAD_PART")?;
    let stub = namespace.get_by_name(&session.upload_id)?;
    stub.fetch_with_request(req).await
}

async fn handle_multipart_start(
    bucket: &Bucket,
    key: &str,
    req: &mut Request,
    env: &Env,
) -> Result<Response> {
    if key.is_empty() {
        return text_response(405, "Cannot replace the root collection");
    }
    if matches!(node_type(bucket, key).await?, Node::Dir) {
        return text_response(405, "A collection already exists at this path");
    }

    let input = parse_json_body::<MultipartStartRequest>(req)
        .await
        .map_err(|error| {
            worker::console_error!("multipart start received invalid JSON for {key:?}: {error}");
            error
        })?;
    let part_count = multipart_part_count(input.size);
    if input.size == 0 {
        return text_response(400, "Use PUT for an empty file");
    }
    if part_count > MAX_MULTIPART_PARTS {
        return text_response(413, "File is too large for the multipart upload limit");
    }

    let upload = bucket
        .create_multipart_upload(key)
        .http_metadata(HttpMetadata {
            content_type: input.content_type,
            ..Default::default()
        })
        .execute()
        .await
        .map_err(|error| {
            worker::console_error!(
                "multipart start could not create R2 upload for {key:?}: {error}"
            );
            error
        })?;
    let session = MultipartSession {
        key: key.to_string(),
        upload_id: upload.upload_id().await,
        size: input.size,
        expires_at: js_sys::Date::now() as u64 + MULTIPART_SESSION_TTL_MILLIS,
    };
    let session_token = encode_multipart_session(&session, env).map_err(|error| {
        worker::console_error!("multipart start could not sign session for {key:?}: {error}");
        error
    })?;
    json_response_with_status(
        201,
        &MultipartStartResponse {
            session: session_token,
            part_size: MULTIPART_PART_SIZE,
        },
    )
}

async fn handle_multipart_part(
    bucket: &Bucket,
    key: &str,
    req: &mut Request,
    env: &Env,
    query: &HashMap<String, String>,
) -> Result<Response> {
    let Some(session) = multipart_session_from_query(query, env)? else {
        return text_response(403, "Invalid or expired multipart upload session");
    };
    if session.key != key {
        return text_response(403, "Multipart upload session does not match this path");
    }
    let Some(part_number) = query
        .get("partNumber")
        .and_then(|value| value.parse::<u16>().ok())
    else {
        return text_response(400, "Invalid multipart part number");
    };
    let Some(expected_length) = multipart_part_length(&session, part_number) else {
        return text_response(400, "Multipart part number is outside the upload range");
    };
    if req
        .headers()
        .get("content-length")?
        .and_then(|value| value.parse::<u64>().ok())
        .is_some_and(|length| length != expected_length)
    {
        return text_response(400, "Multipart part has an unexpected length");
    }

    let stream = req.stream()?;
    let upload = bucket.resume_multipart_upload(key, &session.upload_id)?;
    let uploaded = upload
        .upload_part(
            part_number,
            FixedLengthStream::wrap(stream, expected_length),
        )
        .await?;
    json_response_with_status(
        200,
        &MultipartPartResponse {
            part_number: uploaded.part_number(),
            etag: uploaded.etag(),
        },
    )
}

async fn handle_multipart_complete(
    bucket: &Bucket,
    key: &str,
    req: &mut Request,
    env: &Env,
) -> Result<Response> {
    let input = parse_json_body::<MultipartCompleteRequest>(req).await?;
    let Some(session) = decode_multipart_session(&input.session, env)? else {
        return text_response(403, "Invalid or expired multipart upload session");
    };
    if session.key != key {
        return text_response(403, "Multipart upload session does not match this path");
    }

    let expected_part_count = multipart_part_count(session.size);
    if input.parts.len() as u64 != expected_part_count {
        return text_response(400, "Multipart upload is missing one or more parts");
    }
    let mut parts = Vec::with_capacity(input.parts.len());
    for (index, part) in input.parts.into_iter().enumerate() {
        let expected_part_number = (index + 1) as u16;
        if part.part_number != expected_part_number || part.etag.is_empty() {
            return text_response(400, "Invalid multipart completion part list");
        }
        parts.push(worker::UploadedPart::new(part.part_number, part.etag));
    }

    let upload = bucket.resume_multipart_upload(key, &session.upload_id)?;
    upload.complete(parts).await?;
    Ok(Response::builder().with_status(201).empty())
}

async fn handle_multipart_abort(
    bucket: &Bucket,
    key: &str,
    env: &Env,
    query: &HashMap<String, String>,
) -> Result<Response> {
    let Some(session) = multipart_session_from_query(query, env)? else {
        return text_response(403, "Invalid or expired multipart upload session");
    };
    if session.key != key {
        return text_response(403, "Multipart upload session does not match this path");
    }
    bucket
        .resume_multipart_upload(key, &session.upload_id)?
        .abort()
        .await?;
    Ok(Response::builder().with_status(204).empty())
}

async fn handle_patch(bucket: &Bucket, key: &str, req: &mut Request) -> Result<Response> {
    if req.headers().get("x-update-range")?.as_deref() != Some("append") {
        return text_response(405, "Only X-Update-Range: append is supported");
    }
    let Some(existing) = bucket.get(key).execute().await? else {
        return text_response(404, "Not Found");
    };
    let existing_size = existing.size();
    let incoming = req.bytes().await?;
    if existing_size + incoming.len() as u64 > MAX_PATCH_BYTES {
        return text_response(
            413,
            "Resumable append is limited to 10 MiB in the Worker target; retry the PUT upload",
        );
    }
    let mut bytes = existing
        .body()
        .ok_or_else(|| worker::Error::RustError("R2 object body missing".into()))?
        .bytes()
        .await?;
    bytes.extend_from_slice(&incoming);
    bucket
        .put(key, bytes)
        .http_metadata(upload_metadata(req)?)
        .execute()
        .await?;
    Ok(Response::builder().with_status(204).empty())
}

async fn handle_delete(bucket: &Bucket, key: &str) -> Result<Response> {
    if key.is_empty() {
        return text_response(403, "Cannot delete the root collection");
    }
    match node_type(bucket, key).await? {
        Node::File(_) => bucket.delete(key).await?,
        Node::Dir => delete_prefix(bucket, &directory_prefix(key)).await?,
        Node::Missing => return text_response(404, "Not Found"),
    }
    Ok(Response::builder().with_status(204).empty())
}

async fn handle_mkcol(bucket: &Bucket, key: &str) -> Result<Response> {
    if key.is_empty() || !matches!(node_type(bucket, key).await?, Node::Missing) {
        return text_response(405, "Already exists");
    }
    bucket
        .put(directory_marker(key), Vec::new())
        .http_metadata(HttpMetadata {
            content_type: Some("application/x-dufs-directory-marker".into()),
            ..Default::default()
        })
        .execute()
        .await?;
    Ok(Response::builder().with_status(201).empty())
}

async fn handle_propfind(bucket: &Bucket, key: &str, req: &Request) -> Result<Response> {
    let depth = match req.headers().get("depth")?.as_deref() {
        None | Some("1") => 1,
        Some("0") => 0,
        _ => return text_response(400, "Invalid depth: only 0 and 1 are allowed."),
    };
    let mut xml = String::new();
    match node_type(bucket, key).await? {
        Node::File(object) => xml.push_str(&dav_item_xml(&file_item(key, &object), key)),
        Node::Dir => {
            xml.push_str(&dav_item_xml(
                &PathItem {
                    path_type: PathType::Dir,
                    name: key.rsplit('/').next().unwrap_or_default().to_string(),
                    mtime: 0,
                    size: 0,
                },
                key,
            ));
            if depth == 1 {
                for item in list_dir(bucket, key).await? {
                    let child_key = join_key(key, &item.name);
                    xml.push_str(&dav_item_xml(&item, &child_key));
                }
            }
        }
        Node::Missing => return text_response(404, "Not Found"),
    }
    dav_multistatus(xml)
}

async fn handle_copy_or_move(
    bucket: &Bucket,
    source: &str,
    req: &Request,
    move_source: bool,
) -> Result<Response> {
    if source.is_empty() {
        return text_response(403, "Cannot copy or move the root collection");
    }
    let destination = match destination_key(req)? {
        Some(value) => value,
        None => return text_response(400, "Invalid Destination"),
    };
    if destination.is_empty() || destination == source {
        return text_response(400, "Invalid Destination");
    }
    let source_node = node_type(bucket, source).await?;
    if matches!(source_node, Node::Missing) {
        return text_response(404, "Not Found");
    }
    if matches!(source_node, Node::Dir) && destination.starts_with(&directory_prefix(source)) {
        return text_response(409, "Cannot copy a collection into itself");
    }
    let destination_exists = !matches!(node_type(bucket, &destination).await?, Node::Missing);
    if destination_exists
        && req
            .headers()
            .get("overwrite")?
            .is_some_and(|value| value.eq_ignore_ascii_case("F"))
    {
        return text_response(412, "Destination exists");
    }
    if destination_exists {
        match node_type(bucket, &destination).await? {
            Node::File(_) => bucket.delete(&destination).await?,
            Node::Dir => delete_prefix(bucket, &directory_prefix(&destination)).await?,
            Node::Missing => {}
        }
    }

    match &source_node {
        Node::File(_) => copy_object(bucket, source, &destination).await?,
        Node::Dir => {
            let source_prefix = directory_prefix(source);
            let destination_prefix = directory_prefix(&destination);
            for key in list_all_keys(bucket, &source_prefix).await? {
                let suffix = key.strip_prefix(&source_prefix).unwrap_or_default();
                copy_object(bucket, &key, &format!("{destination_prefix}{suffix}")).await?;
            }
        }
        Node::Missing => unreachable!(),
    }
    if move_source {
        match &source_node {
            Node::File(_) => bucket.delete(source).await?,
            Node::Dir => delete_prefix(bucket, &directory_prefix(source)).await?,
            Node::Missing => {}
        }
    }
    Ok(Response::builder()
        .with_status(if destination_exists { 204 } else { 201 })
        .empty())
}

async fn copy_object(bucket: &Bucket, source: &str, destination: &str) -> Result<()> {
    let object = bucket
        .get(source)
        .execute()
        .await?
        .ok_or_else(|| worker::Error::RustError("source object disappeared".into()))?;
    let size = object.size();
    let metadata = object.http_metadata();
    let stream = object
        .body()
        .ok_or_else(|| worker::Error::RustError("R2 object body missing".into()))?
        .stream()?;
    bucket
        .put(destination, FixedLengthStream::wrap(stream, size))
        .http_metadata(metadata)
        .execute()
        .await?;
    Ok(())
}

async fn file_response(
    bucket: &Bucket,
    key: &str,
    metadata: &Object,
    req: &Request,
    head_only: bool,
    public_share: bool,
) -> Result<Response> {
    let etag = metadata.http_etag();
    if req
        .headers()
        .get("if-none-match")?
        .is_some_and(|value| value == "*" || value.split(',').any(|tag| tag.trim() == etag))
    {
        return Ok(Response::builder().with_status(304).empty());
    }
    if req
        .headers()
        .get("if-match")?
        .is_some_and(|value| value != "*" && !value.split(',').any(|tag| tag.trim() == etag))
    {
        return Ok(Response::builder().with_status(412).empty());
    }

    let size = metadata.size();
    let range = match req.headers().get("range")? {
        Some(value) => match parse_range(&value, size) {
            Ok(range) => Some(range),
            Err(()) => {
                return Ok(Response::builder()
                    .with_status(416)
                    .with_header("content-range", &format!("bytes */{size}"))?
                    .empty())
            }
        },
        None => None,
    };
    let headers = Headers::new();
    let r2_metadata = metadata.http_metadata();
    let content_type = r2_metadata
        .content_type
        .as_deref()
        .unwrap_or_else(|| content_type_for(key));
    let disposition = if public_share {
        public_share_content_disposition(key, content_type)
    } else {
        content_disposition(key)
    };
    headers.set("content-type", content_type)?;
    headers.set("accept-ranges", "bytes")?;
    headers.set("etag", &etag)?;
    headers.set("content-disposition", &disposition)?;
    if public_share {
        // Public files share an origin with authenticated WebDAV. Do not let a
        // user-provided HTML/SVG/JS object become an active same-origin page.
        headers.set("cache-control", "no-cache")?;
        headers.set(
            "content-security-policy",
            "sandbox; default-src 'none'; frame-ancestors 'none'",
        )?;
        headers.set("referrer-policy", "no-referrer")?;
        headers.set("x-content-type-options", "nosniff")?;
    }
    let (status, content_length) = match range {
        Some((start, end)) => {
            headers.set("content-range", &format!("bytes {start}-{end}/{size}"))?;
            (206, end - start + 1)
        }
        None => (200, size),
    };
    headers.set("content-length", &content_length.to_string())?;
    if head_only {
        return Ok(Response::builder()
            .with_status(status)
            .with_headers(headers)
            .empty());
    }
    let mut operation = bucket.get(key);
    if let Some((start, end)) = range {
        operation = operation.range(worker::Range::OffsetWithLength {
            offset: start,
            length: end - start + 1,
        });
    }
    let object = match operation.execute().await? {
        Some(object) => object,
        None => return text_response(404, "Not Found"),
    };
    let body = object
        .body()
        .ok_or_else(|| worker::Error::RustError("R2 object body missing".into()))?
        .response_body()?;
    Ok(Response::builder()
        .with_status(status)
        .with_headers(headers)
        .body(body))
}

fn index_page(
    key: &str,
    mut paths: Vec<PathItem>,
    exists: bool,
    query: &HashMap<String, String>,
    user: &str,
    read_only: bool,
    head_only: bool,
) -> Result<Response> {
    sort_paths(&mut paths, query);
    if query.contains_key("simple") {
        let content = paths
            .iter()
            .map(|item| {
                if item.is_dir() {
                    format!("{}/\n", item.name)
                } else {
                    format!("{}\n", item.name)
                }
            })
            .collect::<String>();
        return fixed_response(
            200,
            "text/plain; charset=utf-8",
            content.into_bytes(),
            head_only,
        );
    }
    let data = IndexData {
        href: href_for_key(key, false),
        kind: DataKind::Index,
        uri_prefix: "/".into(),
        allow_upload: !read_only,
        allow_delete: !read_only,
        allow_search: true,
        // Creating archives requires materialising an entire R2 prefix in Worker memory.
        allow_archive: false,
        dir_exists: exists,
        auth: true,
        user: Some(user.into()),
        paths,
    };
    if query.contains_key("json") {
        return fixed_response(
            200,
            "application/json; charset=utf-8",
            serde_json::to_vec_pretty(&data)?,
            head_only,
        );
    }
    let encoded_data = BASE64.encode(serde_json::to_vec(&data)?);
    let html = INDEX_HTML
        .replace("__ASSETS_PREFIX__", &format!("/{ASSET_PREFIX}"))
        .replace("__INDEX_DATA__", &encoded_data);
    fixed_response(
        200,
        "text/html; charset=utf-8",
        html.into_bytes(),
        head_only,
    )
}

fn edit_page(
    key: &str,
    object: &Object,
    user: &str,
    read_only: bool,
    view: bool,
) -> Result<Response> {
    let metadata = object.http_metadata();
    let content_type = metadata
        .content_type
        .unwrap_or_else(|| content_type_for(key).into());
    let data = EditData {
        href: href_for_key(key, false),
        kind: if view { DataKind::View } else { DataKind::Edit },
        uri_prefix: "/".into(),
        allow_upload: !read_only,
        allow_delete: !read_only,
        auth: true,
        user: Some(user.into()),
        editable: object.size() <= EDITABLE_TEXT_MAX_SIZE && is_text_content_type(&content_type),
    };
    let encoded_data = BASE64.encode(serde_json::to_vec(&data)?);
    let html = INDEX_HTML
        .replace("__ASSETS_PREFIX__", &format!("/{ASSET_PREFIX}"))
        .replace("__INDEX_DATA__", &encoded_data);
    fixed_response(200, "text/html; charset=utf-8", html.into_bytes(), false)
}

async fn node_type(bucket: &Bucket, key: &str) -> Result<Node> {
    if key.is_empty() {
        return Ok(Node::Dir);
    }
    if let Some(object) = bucket.head(key).await? {
        return Ok(Node::File(object));
    }
    if bucket.head(directory_marker(key)).await?.is_some() {
        return Ok(Node::Dir);
    }
    let contents = bucket
        .list()
        .prefix(directory_prefix(key))
        .limit(1)
        .execute()
        .await?;
    if contents.objects().is_empty() && contents.delimited_prefixes().is_empty() {
        Ok(Node::Missing)
    } else {
        Ok(Node::Dir)
    }
}

async fn list_dir(bucket: &Bucket, key: &str) -> Result<Vec<PathItem>> {
    let prefix = directory_prefix(key);
    let listed = bucket
        .list()
        .prefix(&prefix)
        .delimiter("/")
        .limit(MAX_LIST_RESULTS)
        .execute()
        .await?;
    let mut paths = Vec::new();
    for directory in listed.delimited_prefixes() {
        let name = directory
            .strip_prefix(&prefix)
            .unwrap_or(&directory)
            .trim_end_matches('/');
        if !name.is_empty() {
            paths.push(PathItem {
                path_type: PathType::Dir,
                name: name.into(),
                mtime: 0,
                size: 0,
            });
        }
    }
    for object in listed.objects() {
        let name = object
            .key()
            .strip_prefix(&prefix)
            .unwrap_or_default()
            .to_string();
        if name.is_empty() || name == DIRECTORY_MARKER {
            continue;
        }
        paths.push(PathItem {
            path_type: PathType::File,
            name,
            mtime: object.uploaded().as_millis(),
            size: object.size(),
        });
    }
    Ok(paths)
}

async fn search_dir(bucket: &Bucket, key: &str, query: &str) -> Result<Vec<PathItem>> {
    if query.is_empty() {
        return list_dir(bucket, key).await;
    }
    let prefix = directory_prefix(key);
    let needle = query.to_lowercase();
    let mut paths = Vec::new();
    for object_key in list_all_keys_limited(bucket, &prefix, MAX_SEARCH_SCAN).await? {
        let relative = object_key.strip_prefix(&prefix).unwrap_or_default();
        if relative == DIRECTORY_MARKER || !relative.to_lowercase().contains(&needle) {
            continue;
        }
        let Some(object) = bucket.head(&object_key).await? else {
            continue;
        };
        paths.push(PathItem {
            path_type: PathType::File,
            name: relative.into(),
            mtime: object.uploaded().as_millis(),
            size: object.size(),
        });
        if paths.len() >= MAX_LIST_RESULTS as usize {
            break;
        }
    }
    Ok(paths)
}

async fn list_all_keys(bucket: &Bucket, prefix: &str) -> Result<Vec<String>> {
    list_all_keys_limited(bucket, prefix, usize::MAX).await
}

async fn list_all_keys_limited(bucket: &Bucket, prefix: &str, limit: usize) -> Result<Vec<String>> {
    let mut keys = Vec::new();
    let mut cursor = None;
    loop {
        let listed = match cursor.as_deref() {
            Some(cursor) => {
                bucket
                    .list()
                    .prefix(prefix)
                    .limit(MAX_LIST_RESULTS)
                    .cursor(cursor)
                    .execute()
                    .await?
            }
            None => {
                bucket
                    .list()
                    .prefix(prefix)
                    .limit(MAX_LIST_RESULTS)
                    .execute()
                    .await?
            }
        };
        for object in listed.objects() {
            keys.push(object.key());
            if keys.len() >= limit {
                return Ok(keys);
            }
        }
        if !listed.truncated() {
            return Ok(keys);
        }
        cursor = listed.cursor();
        if cursor.is_none() {
            return Ok(keys);
        }
    }
}

async fn delete_prefix(bucket: &Bucket, prefix: &str) -> Result<()> {
    let keys = list_all_keys(bucket, prefix).await?;
    for chunk in keys.chunks(MAX_LIST_RESULTS as usize) {
        bucket.delete_multiple(chunk.to_vec()).await?;
    }
    Ok(())
}

async fn put_request_body(
    bucket: &Bucket,
    key: &str,
    req: &mut Request,
    metadata: HttpMetadata,
) -> Result<()> {
    let content_length = req
        .headers()
        .get("content-length")?
        .and_then(|value| value.parse::<u64>().ok());
    match content_length {
        Some(length) => {
            let stream = req.stream()?;
            bucket
                .put(key, FixedLengthStream::wrap(stream, length))
                .http_metadata(metadata)
                .execute()
                .await?;
        }
        None => {
            let bytes = req.bytes().await?;
            bucket
                .put(key, bytes)
                .http_metadata(metadata)
                .execute()
                .await?;
        }
    }
    Ok(())
}

fn upload_metadata(req: &Request) -> Result<HttpMetadata> {
    Ok(HttpMetadata {
        content_type: req.headers().get("content-type")?,
        ..Default::default()
    })
}

fn options_response() -> Result<Response> {
    let headers = dav_headers()?;
    Ok(Response::builder().with_headers(headers).empty())
}

fn unauthorized_response() -> Result<Response> {
    let headers = dav_headers()?;
    headers.set(
        "www-authenticate",
        "Basic realm=\"DUFS\", charset=\"UTF-8\"",
    )?;
    Ok(Response::builder()
        .with_status(401)
        .with_headers(headers)
        .empty())
}

fn dav_headers() -> Result<Headers> {
    let headers = Headers::new();
    headers.set(
        "allow",
        "GET, HEAD, POST, PUT, OPTIONS, DELETE, PATCH, PROPFIND, PROPPATCH, MKCOL, COPY, MOVE, LOCK, UNLOCK",
    )?;
    headers.set("dav", "1, 2, 3")?;
    headers.set("ms-author-via", "DAV")?;
    Ok(headers)
}

fn handle_lock(key: &str) -> Result<Response> {
    let token = format!("opaquelocktoken:dufs-r2-{}", js_sys::Date::now() as u64);
    let body = format!(
        r#"<?xml version="1.0" encoding="utf-8"?>
<D:prop xmlns:D="DAV:"><D:lockdiscovery><D:activelock>
<D:locktoken><D:href>{token}</D:href></D:locktoken>
<D:lockroot><D:href>{}</D:href></D:lockroot>
</D:activelock></D:lockdiscovery></D:prop>"#,
        xml_escape(&href_for_key(key, false))
    );
    Ok(Response::builder()
        .with_header("content-type", "application/xml; charset=utf-8")?
        .with_header("lock-token", &format!("<{token}>"))?
        .fixed(body.into_bytes()))
}

fn proppatch_response(key: &str) -> Result<Response> {
    dav_multistatus(format!(
        r#"<D:response><D:href>{}</D:href><D:propstat><D:prop></D:prop><D:status>HTTP/1.1 403 Forbidden</D:status></D:propstat></D:response>"#,
        xml_escape(&href_for_key(key, false))
    ))
}

fn dav_multistatus(content: String) -> Result<Response> {
    let body = format!(
        r#"<?xml version="1.0" encoding="utf-8" ?>
<D:multistatus xmlns:D="DAV:">{content}</D:multistatus>"#
    );
    Ok(Response::builder()
        .with_status(207)
        .with_header("content-type", "application/xml; charset=utf-8")?
        .fixed(body.into_bytes()))
}

fn dav_item_xml(item: &PathItem, key: &str) -> String {
    let mut href = href_for_key(key, item.is_dir());
    if item.is_dir() && !href.ends_with('/') {
        href.push('/');
    }
    let modified = if item.mtime == 0 {
        String::new()
    } else {
        js_sys::Date::new(&wasm_bindgen::JsValue::from_f64(item.mtime as f64))
            .to_utc_string()
            .into()
    };
    let display_name = xml_escape(item.name.rsplit('/').next().unwrap_or_default());
    let resource_type = if item.is_dir() {
        "<D:resourcetype><D:collection/></D:resourcetype>"
    } else {
        "<D:resourcetype></D:resourcetype>"
    };
    let content_length = if item.is_dir() {
        String::new()
    } else {
        format!("<D:getcontentlength>{}</D:getcontentlength>", item.size)
    };
    format!(
        r#"<D:response><D:href>{}</D:href><D:propstat><D:prop><D:displayname>{display_name}</D:displayname>{content_length}<D:getlastmodified>{modified}</D:getlastmodified>{resource_type}</D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>"#,
        xml_escape(&href)
    )
}

fn file_item(key: &str, object: &Object) -> PathItem {
    PathItem {
        path_type: PathType::File,
        name: key.rsplit('/').next().unwrap_or_default().to_string(),
        mtime: object.uploaded().as_millis(),
        size: object.size(),
    }
}

async fn parse_json_body<T: DeserializeOwned>(req: &mut Request) -> Result<T> {
    let body = req.bytes().await?;
    serde_json::from_slice(&body)
        .map_err(|error| worker::Error::RustError(format!("Invalid JSON request body: {error}")))
}

fn json_response<T: Serialize>(value: &T) -> Result<Response> {
    json_response_with_status(200, value)
}

fn json_response_with_status<T: Serialize>(status: u16, value: &T) -> Result<Response> {
    Ok(Response::builder()
        .with_status(status)
        .with_header("content-type", "application/json; charset=utf-8")?
        .fixed(serde_json::to_vec(value)?))
}

fn text_response(status: u16, message: &str) -> Result<Response> {
    Ok(Response::builder()
        .with_status(status)
        .with_header("content-type", "text/plain; charset=utf-8")?
        .fixed(message.as_bytes().to_vec()))
}

fn fixed_response(
    status: u16,
    content_type: &str,
    body: Vec<u8>,
    head_only: bool,
) -> Result<Response> {
    let builder = Response::builder()
        .with_status(status)
        .with_header("content-type", content_type)?
        .with_header("content-length", &body.len().to_string())?
        .with_header("cache-control", "no-cache")?;
    if head_only {
        Ok(builder.empty())
    } else {
        Ok(builder.fixed(body))
    }
}

fn destination_key(req: &Request) -> Result<Option<String>> {
    let Some(destination) = req.headers().get("destination")? else {
        return Ok(None);
    };
    let path = worker::Url::parse(&destination)
        .map(|url| url.path().to_string())
        .unwrap_or(destination);
    Ok(normalize_key(&path).ok())
}

fn query_params(req: &Request) -> Result<HashMap<String, String>> {
    Ok(req
        .url()?
        .query_pairs()
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect())
}

fn normalize_key(path: &str) -> std::result::Result<String, ()> {
    let decoded = percent_decode_str(path).decode_utf8().map_err(|_| ())?;
    let mut parts = Vec::new();
    for part in decoded.split('/') {
        if part.is_empty() {
            continue;
        }
        if matches!(part, "." | "..") || part == DIRECTORY_MARKER || part.contains('\0') {
            return Err(());
        }
        parts.push(part);
    }
    Ok(parts.join("/"))
}

fn directory_prefix(key: &str) -> String {
    if key.is_empty() {
        String::new()
    } else {
        format!("{key}/")
    }
}

fn directory_marker(key: &str) -> String {
    format!("{}{DIRECTORY_MARKER}", directory_prefix(key))
}

fn join_key(parent: &str, child: &str) -> String {
    if parent.is_empty() {
        child.to_string()
    } else {
        format!("{parent}/{child}")
    }
}

fn href_for_key(key: &str, trailing_slash: bool) -> String {
    let mut href = if key.is_empty() {
        "/".to_string()
    } else {
        format!(
            "/{}",
            key.split('/')
                .map(|part| utf8_percent_encode(part, NON_ALPHANUMERIC).to_string())
                .collect::<Vec<_>>()
                .join("/")
        )
    };
    if trailing_slash && !href.ends_with('/') {
        href.push('/');
    }
    href
}

fn parse_range(value: &str, size: u64) -> std::result::Result<(u64, u64), ()> {
    let value = value.strip_prefix("bytes=").ok_or(())?;
    if value.contains(',') || size == 0 {
        return Err(());
    }
    let (start, end) = value.split_once('-').ok_or(())?;
    if start.is_empty() {
        let suffix = end.parse::<u64>().map_err(|_| ())?;
        if suffix == 0 {
            return Err(());
        }
        return Ok((size.saturating_sub(suffix), size - 1));
    }
    let start = start.parse::<u64>().map_err(|_| ())?;
    if start >= size {
        return Err(());
    }
    let end = if end.is_empty() {
        size - 1
    } else {
        end.parse::<u64>().map_err(|_| ())?.min(size - 1)
    };
    if end < start {
        return Err(());
    }
    Ok((start, end))
}

fn sort_paths(paths: &mut [PathItem], query: &HashMap<String, String>) {
    match query.get("sort").map(String::as_str) {
        Some("mtime") => paths.sort_by_key(|item| (item.path_type as u8, item.mtime)),
        Some("size") => paths.sort_by_key(|item| (item.path_type as u8, item.size)),
        _ => paths.sort_by_key(|item| (item.path_type as u8, item.name.to_lowercase())),
    }
    if query.get("order").is_some_and(|order| order == "desc") {
        paths.reverse();
    }
}

fn content_type_for(key: &str) -> &'static str {
    match key
        .rsplit('.')
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "html" | "htm" => "text/html; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "js" | "mjs" => "application/javascript; charset=utf-8",
        "json" => "application/json; charset=utf-8",
        "txt" | "md" | "rs" | "toml" | "yaml" | "yml" => "text/plain; charset=utf-8",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "pdf" => "application/pdf",
        "zip" => "application/zip",
        "mp3" => "audio/mpeg",
        "mp4" => "video/mp4",
        _ => "application/octet-stream",
    }
}

fn is_text_content_type(value: &str) -> bool {
    value.starts_with("text/")
        || value.starts_with("application/json")
        || value.starts_with("application/javascript")
        || value.starts_with("application/xml")
}

fn content_disposition(key: &str) -> String {
    content_disposition_with_type(key, "inline")
}

fn public_share_content_disposition(key: &str, content_type: &str) -> String {
    // Only passive media types are safe to render in the WebDAV origin.
    // Everything else is downloaded instead of interpreted by the browser.
    let inline = content_type.starts_with("image/")
        && !content_type.eq_ignore_ascii_case("image/svg+xml")
        || content_type.starts_with("audio/")
        || content_type.starts_with("video/")
        || content_type.eq_ignore_ascii_case("application/pdf")
        || content_type.starts_with("text/plain");
    content_disposition_with_type(key, if inline { "inline" } else { "attachment" })
}

fn content_disposition_with_type(key: &str, disposition: &str) -> String {
    let name = key
        .rsplit('/')
        .next()
        .unwrap_or("download")
        .replace(['\r', '\n', '"'], "_");
    format!("{disposition}; filename=\"{name}\"")
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn html_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#x27;")
}

fn token_signature(key: &str, expiry: u64, username: &str, password: &str) -> String {
    let mut mac = HmacSha256::new_from_slice(format!("{username}:{password}").as_bytes())
        .expect("HMAC accepts arbitrary key lengths");
    mac.update(key.as_bytes());
    mac.update(b"\n");
    mac.update(expiry.to_string().as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

fn verify_download_token(token: &str, key: &str, username: &str, password: &str) -> bool {
    let Some((expiry, signature)) = token.split_once('.') else {
        return false;
    };
    let Ok(expiry) = expiry.parse::<u64>() else {
        return false;
    };
    if expiry < js_sys::Date::now() as u64 {
        return false;
    }
    constant_time_eq(
        signature.as_bytes(),
        token_signature(key, expiry, username, password).as_bytes(),
    )
}

fn encode_multipart_session(session: &MultipartSession, env: &Env) -> Result<String> {
    let username = env.secret("DUFS_USERNAME")?.to_string();
    let password = env.secret("DUFS_PASSWORD")?.to_string();
    let payload = BASE64_URL.encode(serde_json::to_vec(session)?);
    let signature = multipart_session_signature(&payload, &username, &password);
    Ok(format!("{payload}.{signature}"))
}

fn decode_multipart_session(token: &str, env: &Env) -> Result<Option<MultipartSession>> {
    let Some((payload, signature)) = token.split_once('.') else {
        return Ok(None);
    };
    let username = env.secret("DUFS_USERNAME")?.to_string();
    let password = env.secret("DUFS_PASSWORD")?.to_string();
    if !constant_time_eq(
        signature.as_bytes(),
        multipart_session_signature(payload, &username, &password).as_bytes(),
    ) {
        return Ok(None);
    }
    let Ok(bytes) = BASE64_URL.decode(payload) else {
        return Ok(None);
    };
    let Ok(session) = serde_json::from_slice::<MultipartSession>(&bytes) else {
        return Ok(None);
    };
    if session.expires_at < js_sys::Date::now() as u64
        || session.size == 0
        || multipart_part_count(session.size) > MAX_MULTIPART_PARTS
    {
        return Ok(None);
    }
    Ok(Some(session))
}

fn multipart_session_from_query(
    query: &HashMap<String, String>,
    env: &Env,
) -> Result<Option<MultipartSession>> {
    match query.get("session") {
        Some(token) => decode_multipart_session(token, env),
        None => Ok(None),
    }
}

fn multipart_session_signature(payload: &str, username: &str, password: &str) -> String {
    let mut mac = HmacSha256::new_from_slice(format!("{username}:{password}").as_bytes())
        .expect("HMAC accepts arbitrary key lengths");
    mac.update(b"dufs-r2-multipart\n");
    mac.update(payload.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

fn multipart_part_count(size: u64) -> u64 {
    size.div_ceil(MULTIPART_PART_SIZE)
}

fn multipart_part_length(session: &MultipartSession, part_number: u16) -> Option<u64> {
    let part_number = part_number as u64;
    let part_count = multipart_part_count(session.size);
    if part_number == 0 || part_number > part_count {
        return None;
    }
    let offset = (part_number - 1) * MULTIPART_PART_SIZE;
    Some((session.size - offset).min(MULTIPART_PART_SIZE))
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let max_len = left.len().max(right.len());
    let mut difference = left.len() ^ right.len();
    for index in 0..max_len {
        difference |= (left.get(index).copied().unwrap_or(0)
            ^ right.get(index).copied().unwrap_or(0)) as usize;
    }
    difference == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_normalisation_blocks_traversal_and_reserved_markers() {
        assert_eq!(
            normalize_key("/docs/a%20file.txt"),
            Ok("docs/a file.txt".into())
        );
        assert!(normalize_key("/docs/%2e%2e/secret").is_err());
        assert!(normalize_key("/.dufs-directory").is_err());
    }

    #[test]
    fn public_share_scope_is_exactly_the_share_prefix() {
        assert!(is_public_share_key("share"));
        assert!(is_public_share_key("share/reports/q1.pdf"));
        assert!(!is_public_share_key("shares/report.pdf"));
        assert!(!is_public_share_key("private/share/report.pdf"));
    }

    #[test]
    fn public_share_downloads_active_content() {
        assert!(
            public_share_content_disposition("share/photo.png", "image/png").starts_with("inline;")
        );
        assert!(
            public_share_content_disposition("share/readme.txt", "text/plain; charset=utf-8")
                .starts_with("inline;")
        );
        assert!(
            public_share_content_disposition("share/page.html", "text/html; charset=utf-8")
                .starts_with("attachment;")
        );
        assert!(
            public_share_content_disposition("share/logo.svg", "image/svg+xml")
                .starts_with("attachment;")
        );
    }

    #[test]
    fn public_share_file_rows_include_download_buttons() {
        let file_row = public_share_item_row(
            "share",
            &PathItem {
                path_type: PathType::File,
                name: "report & q1.pdf".into(),
                mtime: 0,
                size: 42,
            },
        );
        assert!(file_row.contains(r#"class="download""#));
        assert!(file_row.contains(r#"download aria-label="Download report &amp; q1.pdf""#));
        assert!(file_row.contains(r#">42 bytes</span>"#));

        let dir_row = public_share_item_row(
            "share",
            &PathItem {
                path_type: PathType::Dir,
                name: "reports".into(),
                mtime: 0,
                size: 0,
            },
        );
        assert!(!dir_row.contains(r#"class="download""#));
        assert!(dir_row.contains(r#">folder</span>"#));
    }

    #[test]
    fn byte_range_supports_common_single_range_forms() {
        assert_eq!(parse_range("bytes=0-4", 10), Ok((0, 4)));
        assert_eq!(parse_range("bytes=5-", 10), Ok((5, 9)));
        assert_eq!(parse_range("bytes=-3", 10), Ok((7, 9)));
        assert!(parse_range("bytes=10-11", 10).is_err());
    }

    #[test]
    fn multipart_parts_are_fixed_size_except_for_the_final_part() {
        let session = MultipartSession {
            key: "archive.bin".into(),
            upload_id: "upload-id".into(),
            size: MULTIPART_PART_SIZE * 2 + 123,
            expires_at: u64::MAX,
        };
        assert_eq!(multipart_part_count(session.size), 3);
        assert_eq!(
            multipart_part_length(&session, 1),
            Some(MULTIPART_PART_SIZE)
        );
        assert_eq!(
            multipart_part_length(&session, 2),
            Some(MULTIPART_PART_SIZE)
        );
        assert_eq!(multipart_part_length(&session, 3), Some(123));
        assert_eq!(multipart_part_length(&session, 4), None);
    }
}
