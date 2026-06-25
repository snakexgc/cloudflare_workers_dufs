# Dufs WebDAV：Cloudflare Workers + R2 部署手册

这个目录是 Dufs 的独立 Rust/WASM Worker。它不是原生 Dufs CLI 的替代品：原项目使用
本地文件系统和 TCP 监听，而本 Worker 通过 Cloudflare 的 Fetch Runtime 接收请求，并将文件
存入 R2。


## 运行方式与资源映射

- Worker 名称：`dufs-r2`；在 `wrangler.toml` 的 `name` 中配置。
- R2 binding：`DUFS_BUCKET`；当前指向 `dufs-files` bucket。
- 文件：R2 中同名的 object key。
- 目录：R2 object key 的前缀；`MKCOL` 会额外创建隐藏的 `.dufs-directory` marker，以保留空目录。
- 身份验证：HTTP Basic Auth；用户名和密码只从 Worker Secrets 读取。

支持 `GET`、`HEAD`、`POST`、`PUT`、`PATCH`（仅 append）、`DELETE`、`OPTIONS`、
`PROPFIND`、`PROPPATCH`、`MKCOL`、`COPY`、`MOVE`、`LOCK` 和 `UNLOCK`。浏览器 UI 的认证检查
使用 `POST ?__dufs_checkauth=1`，避免 Cloudflare 对私有 `CHECKAUTH` HTTP 方法返回 501。

## 网页端与 WebDAV 上传

Cloudflare 免费 Worker 会在请求进入 Worker 前拒绝超过 100 MB 的请求体，因此标准 WebDAV 的单次
`PUT` 无法透明地上传超大文件；同时，几十 MiB 的单次 `PUT` 也容易消耗入口 Worker 的 CPU 预算。
超过 100 MB 的请求体硬限制不能通过改写 Worker 内的 `PUT` 处理逻辑绕过。

网页端已针对大文件自动切换到 R2 Multipart Upload：

1. 文件大于 16 MiB 时，浏览器通过标准 `POST` 创建一个 R2 multipart session。
2. 浏览器将文件分成 16 MiB 分片，每个分片通过独立的 `PUT` 请求流式写入 R2。
3. 所有分片上传完成后，浏览器通过 `POST` 请求合并为一个 R2 object。

每个请求都远小于 100 MB，因此可上传远大于 100 MB 的文件。上传 session 由签名令牌和 R2 的
multipart upload ID 维护；每个 session 的分片写入会路由到一个 Durable Object。它不保存文件数据，
只将流式写入移出入口 Worker 的 10 ms CPU 限制并串行化同一上传的分片。失败后点击重试会安全地创建新的 session。

网页端的小文件 `PUT`、第三方 WebDAV 的标准 `PUT`，以及 `PATCH` append 续传都会按目标 object key
路由到 `ObjectUpload` Durable Object，再由该 Durable Object 写入 R2。这样 60 MB 左右的 WebDAV 单次上传
不再由入口 Worker 承担流式写入 CPU；同一路径的写入也会自然串行化。

已认证 WebDAV 的 `COPY` 与 `MOVE`（包括网页中的 **Move & Rename**）也会根据源文件路由到独立的
Durable Object。这样重命名或移动大文件时，R2 的流式复制不会耗尽入口 Worker 的 CPU 预算；同一源文件的
并发移动会串行化，互不相关的文件不会互相阻塞。

部署更新后，请重新加载网页；如果页面在部署前已经打开，按 `Ctrl+F5` 强制刷新。旧脚本即使继续尝试单次
`PUT`，请求也会进入 `ObjectUpload` Durable Object，而不是由入口 Worker 直接写入 R2。

Multipart 是网页端扩展，不是标准 WebDAV 的一部分。第三方 WebDAV 客户端仍受单个请求体 100 MB
硬限制；超过该大小请使用网页端上传，或改用能直接访问 R2 S3 API 的专用客户端。

## 公开分享目录：`/share/`

同一个 Worker 提供一个无需登录的只读分享页。它只映射 R2 中以 `share/` 开头的 object key，不会公开
bucket 中的其他文件：

- 将要分享的文件通过已认证的 WebDAV 放进 `share/`，例如 R2 key 为 `share/manual.pdf`。
- 外部用户无需用户名或密码即可访问 `https://dav.snakexgc.com/share/manual.pdf`；访问
  `https://dav.snakexgc.com/share/` 会看到仅列出该前缀内容的目录页。
- 未携带 `Authorization` 的 `/share/` 请求只接受 `GET` 和 `HEAD`；`PUT`、`DELETE`、`MOVE`、
  `PROPFIND` 等都会返回 `405 Method Not Allowed`，因此公开访问者无法修改任何文件。
- 已携带有效 HTTP Basic Auth 的请求仍按原有 WebDAV 流程处理。管理员可以从根目录进入 `share/`，并继续
  上传、删除、移动和创建其中的文件；这不会影响其他 WebDAV 路径。
- 目录和文件响应使用 `Cache-Control: no-cache`。删除或移动分享文件即可撤销其公开链接，客户端下次读取时会
  向 Worker 重新验证内容。

公开目录与已认证 WebDAV 共用同一个域名。为避免分享内容成为同源脚本入口，分享页不加载 WebDAV 前端脚本，
并为文件设置 `nosniff` 和 CSP sandbox；HTML、JavaScript、SVG 等主动内容强制下载而不是在该域名中执行。
图片（不含 SVG）、音视频、PDF 和纯文本可直接预览。

不要把整个 `dufs-files` bucket 配置为 R2 Public Bucket：该方案会按 bucket 而不是按 `share/` 前缀公开数据，
无法满足此处的隔离要求。若以后需要面向大量匿名下载的独立缓存域名，可把分享文件复制到单独的 public bucket，
并使用专用域名；当前的前缀路由方案不需要额外存储、复制流程或域名配置。

## 非常重要：必须在本目录运行 Wrangler

本仓库根目录的 `wrangler.jsonc` 是另一个“静态资产 Worker”配置。静态资产 Worker 不能绑定
R2、Secrets 或环境变量；这也是控制台提示“不能将变量添加到只有静态资产的 Worker”的原因。

**始终进入本目录，并在所有 Worker 管理命令中显式传入 `--config .\wrangler.toml`。**

父目录还存在一个静态资产专用的 `wrangler.jsonc`。Wrangler 的自动配置发现可能会选中它，即使当前
终端已经位于本目录；显式 `--config` 才能保证命令操作的是模块 Worker `dufs-r2`。

```powershell
Set-Location D:\gitrepo\dufs_workers\workers\dufs-r2-worker
```

这里的 `wrangler.toml` 指定 `main = "build/index.js"`。该文件由 Rust/WASM 生成的模块
Worker 使用，不是 Static Assets Worker。

## 首次部署

### 1. 安装本机依赖

需要 Node.js、一个已登录的 Cloudflare Wrangler，以及带 WASM target 的 Rust。

```powershell
Set-Location .\workers\dufs-r2-worker

# 仅第一次需要：安装 Rust 的 Workers 编译目标。
rustup target add wasm32-unknown-unknown

# 确认登录的 Cloudflare 账户与权限。
npx wrangler whoami
```

如果 `whoami` 未显示目标账户，先执行并在浏览器中完成授权：

```powershell
npx wrangler login
```

### 2. 创建或确认 R2 bucket（本地手工部署时）

当前配置使用 `dufs-files`。先列出现有 bucket：

```powershell
npx wrangler r2 bucket list
```

若不存在 `dufs-files`，创建它：

```powershell
npx wrangler r2 bucket create dufs-files
```

使用下文的 GitHub Actions 首次部署时无需执行本节：工作流会从 `wrangler.toml` 读取 `bucket_name`，并在
bucket 不存在时自动创建。

如果要使用其他 bucket，请先创建它，再编辑 `wrangler.toml` 的两个字段：

```toml
bucket_name = "你的-bucket-名称"
preview_bucket_name = "你的-bucket-名称"
```

### 3. 选择一个普通模块 Worker 名称

打开 `wrangler.toml`，确认：

```toml
name = "dufs-r2"
main = "build/index.js"
```

如果 `dufs-r2` 已在控制台被创建为“仅静态资产”的 Worker，不要向它添加变量；将 `name` 改为一个
未使用的名称，例如 `dufs-r2-webdav`。这会创建一个新的模块 Worker。

### 4. 本地检查与部署预览

```powershell
# 执行纯 Rust 单元测试。
cargo test

# 编译 WASM、生成 Worker 包，但不上传。
npx wrangler deploy --config .\wrangler.toml --dry-run
```

成功输出必须包含类似以下绑定，而不是只显示 Static Assets：

```text
env.DUFS_BUCKET (...)  R2 Bucket
```

### 5. 首次上传 Worker

```powershell
npx wrangler deploy --config .\wrangler.toml
```

命令完成后会输出 `workers.dev` 地址。首次部署时 Worker 还没有认证凭据，外部请求会返回 500；下一步
写入 Secrets 后立即恢复正常。

### 6. 写入认证 Secrets（本地手工部署时）

下面两条命令会交互式读取输入；输入内容不会写入 `wrangler.toml`、Git 或终端命令历史。

```powershell
npx wrangler secret put DUFS_USERNAME --config .\wrangler.toml
npx wrangler secret put DUFS_PASSWORD --config .\wrangler.toml
```

每次命令提示输入时，分别填入所需的用户名和高强度密码。`secret put` 会直接生成新的生效版本，
**不需要**再执行一次部署命令。

使用 GitHub Actions 时，请不要在 CI 之外重复执行此步骤；改为配置下文的 GitHub `DUFS_USERNAME`、
`DUFS_PASSWORD` Secrets，工作流会在首次部署和每次后续部署中同步它们。

不要把密码写进 `[vars]`、`.toml`、README 或源代码。当前账号已配置过 Secrets；如需轮换密码，重复执行
第二条命令即可。

## 完整部署流程与 GitHub Actions

部署分为“首次自动部署”和“后续自动更新”两部分。Worker 配置、R2 binding、Durable Object migration、
自定义路由均在 `wrangler.toml` 中版本化。工作流可在一个全新的 Cloudflare account 中创建 R2 bucket、
Worker、Durable Objects 和路由；唯一无法自动创建的是 Cloudflare zone/域名本身，因此 `snakexgc.com` 必须已
托管在目标 Cloudflare account 中。

首次自动部署顺序如下：

1. 确认 `wrangler.toml` 中的 Worker 名称、R2 bucket 名称和 `dav.snakexgc.com/*` 路由符合目标环境。
2. 在 GitHub 仓库中配置下面四项 Actions 凭据，并把 `.github/workflows/deploy-dufs-r2.yml` 推送到 `main`。
3. 工作流先从 `wrangler.toml` 读取 `bucket_name`：bucket 存在则复用，不存在则创建。
4. Wrangler 编译 Rust/WASM，首次创建 Worker、应用 `v1`、`v2`、`v3` Durable Object migrations，并创建或更新路由。
5. 工作流将 GitHub 中的 WebDAV 凭据作为 Cloudflare Worker Secrets 随该版本部署；明文不会写入仓库或日志。
6. 之后，影响 Worker 的提交进入 Pull Request 时只执行校验；合并或直接推送到 `main` 后沿用同一流程更新。

工作流的执行顺序是：

```text
Pull Request ──> cargo test + WASM cargo check

main push / manual run ──> 同样的校验 ──> 读取/创建 R2 bucket
                                      ──> worker-build 编译 Rust/WASM
                                      ──> Wrangler deploy --keep-vars --secrets-file
                                      ──> 首次创建或后续更新 Worker、路由与 Durable Object migrations
```

工作流会先进入 `workers/dufs-r2-worker` 作为运行目录，再显式使用 `--config ./wrangler.toml`，
因此不会误用仓库根目录的静态资产 `wrangler.jsonc`。`--keep-vars` 会保留 Dashboard 中单独设置的非机密变量；`--secrets-file` 只会添加或
更新 `DUFS_USERNAME`、`DUFS_PASSWORD`，不会删除其他 Cloudflare Worker Secrets。

### GitHub Actions 所需凭据

在 GitHub 仓库进入 `Settings` → `Secrets and variables` → `Actions`，添加：

| 类型 | 名称 | 是否必需 | 用途 |
| --- | --- | --- | --- |
| Repository secret 或 `production` environment secret | `CLOUDFLARE_API_TOKEN` | 是 | 允许 Wrangler 管理 R2、Worker、路由和 Durable Objects。 |
| Repository variable 或 `production` environment variable | `CLOUDFLARE_ACCOUNT_ID` | 是 | 指定部署到的 Cloudflare 账号；这不是密码，可作为变量保存。 |
| Repository secret 或 `production` environment secret | `DUFS_USERNAME` | 是 | 首次创建、后续保持或轮换 Cloudflare Worker 的 WebDAV 用户名。 |
| Repository secret 或 `production` environment secret | `DUFS_PASSWORD` | 是 | 首次创建、后续保持或轮换 Cloudflare Worker 的 WebDAV 密码。 |

请在 Cloudflare Dashboard 的 `My Profile` → `API Tokens` 创建**自定义 API Token**，并将资源限定到当前
Cloudflare account，以及 `snakexgc.com` 这个 zone。当前配置至少需要以下权限：

| 资源范围 | 权限 | 原因 |
| --- | --- | --- |
| Account | `Workers Scripts: Edit` | 上传 Rust/WASM Worker，并应用 Durable Object migration。 |
| Account | `Workers R2 Storage: Edit` | 使用并校验 `dufs-files` R2 binding。 |
| Zone | `Zone: Read` | 根据 `zone_name` 找到 `snakexgc.com`。 |
| Zone | `Workers Routes: Edit` | 更新 `dav.snakexgc.com/*` 路由。 |

不需要 R2 S3 access key 或 KV 凭据；当前项目既不使用 KV，也不通过 S3 API 部署。工作流会在临时文件中
生成 `DUFS_USERNAME`、`DUFS_PASSWORD` 的 JSON secrets 文件，交给 Wrangler 后立即删除；不要将它们放入
`wrangler.toml`、`.dev.vars`、代码或普通 GitHub Variables。将 API Token 限制为上述 account/zone，且不要
使用 Cloudflare Global API Key。

`CLOUDFLARE_ACCOUNT_ID` 可以在 Cloudflare Dashboard 右侧的账号信息中取得。工作流会在该变量或 API Token
缺失时立即失败，不会退回到交互式 `wrangler login`。

### 自动部署与回滚

将本次修改提交并推送到 `main` 后，GitHub Actions 中的 **Deploy Dufs R2 Worker** 会自动发布。也可在
`Actions` → `Deploy Dufs R2 Worker` → `Run workflow` 中手动触发；手动触发会部署 GitHub 页面中选中的
ref，生产环境通常选择 `main`。

发布完成后，Actions 日志会显示 Worker 版本 ID。若需要回滚，使用本地已登录 Wrangler 执行：

```powershell
Set-Location D:\gitrepo\dufs_workers\workers\dufs-r2-worker
npx wrangler deployments list --name dufs-r2 --config .\wrangler.toml
npx wrangler rollback VERSION_ID --config .\wrangler.toml --message "回滚原因"
```

## 日常更新与监控

源代码、依赖、R2 绑定或 `DUFS_READ_ONLY` 改动后，依次运行：

```powershell
Set-Location D:\gitrepo\dufs_workers\workers\dufs-r2-worker
cargo test
npx wrangler deploy --config .\wrangler.toml --dry-run
npx wrangler deploy --config .\wrangler.toml --keep-vars
```

实时查看请求和异常日志：

```powershell
npx wrangler tail dufs-r2 --config .\wrangler.toml --format pretty
```

按 `Ctrl+C` 停止日志会话。查看已部署版本：

```powershell
npx wrangler deployments list --name dufs-r2
```

回滚到列表中的某个版本 ID：

```powershell
npx wrangler rollback VERSION_ID --config .\wrangler.toml --message "回滚原因"
```

## 本地开发

在本目录创建未跟踪的 `.dev.vars` 文件：

```ini
DUFS_USERNAME=local-admin
DUFS_PASSWORD=local-development-password
```

然后启动本地 Worker：

```powershell
npx wrangler dev --config .\wrangler.toml
```

`.dev.vars` 已被 `.gitignore` 忽略，仍应使用与生产环境不同的密码。

## 可选配置

### 只读模式

将 `wrangler.toml` 改为：

```toml
[vars]
DUFS_READ_ONLY = "true"
```

然后部署：

```powershell
npx wrangler deploy --config .\wrangler.toml
```

此设置会禁止上传、删除、移动、复制和创建目录；认证与读取仍然可用。

### 自定义域名

先完成上面的首次部署，再在 Cloudflare Dashboard 中进入：

`Workers & Pages` → `dufs-r2` → `Settings` → `Domains & Routes`

添加自定义域名或路由后，后续请求可使用该域名；无需修改 R2 bucket 或 Secrets。

## Worker 环境限制

- R2 不是文件系统，目录移动/复制由“复制对象再删除源对象”组成，整个目录操作不是事务性的。
- `?zip` 目录下载与 `?hash` 文件散列被禁用，避免在 Worker 内存中读取任意大的 R2 数据。
- 标准 WebDAV `PUT` 会经由 `ObjectUpload` Durable Object 写入 R2，但仍受 Cloudflare 免费计划的 100 MB 请求体硬限制；网页端会自动使用 multipart。
- `PATCH` 会经由 `ObjectUpload` Durable Object 处理，仅用于小文件 append 续传，且限制为 10 MiB；大文件请使用网页端 multipart 上传。
- 不要删除 `.dufs-directory` 对象；它是空目录 marker，由 Worker 自动管理。
