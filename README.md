# Next Gen Forward v1.6.0
<img width="535" height="185" alt="项目主图 小" src="https://github.com/user-attachments/assets/a632a655-0b6e-4f31-b6ea-1364028bf540" />

这是一个基于 Cloudflare Workers 部署的 Telegram 双向私聊机器人，通过群组话题管理私聊，免费、安全、高效。

支持本地题库 + Cloudflare Turnstile（可选）双重人机验证方式，一键切换。

支持本地规则 + Workers AI（可选）双重垃圾消息拦截，一键开关。

内置设置面板，便于在群组内直接控制机器人各项功能。

本项目基于 [telegram_private_chatbot](https://github.com/jikssha/telegram_private_chatbot) ，进行了大幅修改，并更新保姆级部署教程。

在此对原项目作者 [Vaghr](https://github.com/jikssha) 以及我的好兄弟 打钱 & 逆天 表示特别感谢！

[![License](https://img.shields.io/badge/License-MIT-blue.svg)](https://github.com/mole404/NextGenForward?tab=MIT-1-ov-file)
[![Telegram](https://img.shields.io/badge/Telegram-DM-blue?style=social&logo=telegram)](https://t.me/Arona_Chat_Bot) 

---

## 界面演示：

<img width="652" height="656" alt="界面示例" src="https://github.com/user-attachments/assets/bd89d509-fdef-4376-b1dd-59eec5911834" />


## 目录

* [✨ 核心特性](#-核心特性)
* [💻 指令说明](#-指令说明)
* [📝 前期准备](#-前期准备)
* [🚀 部署方法（二选一）](#-部署方法二选一)
* [🔗 关联和绑定](#-关联和绑定)
* [⚙️ 一些额外的配置（仅了解即可）](#%EF%B8%8F-一些额外的配置仅了解即可)
* [❓ 常见问题及解决方法](#-常见问题及解决方法)


---

## ✨ 核心特性


| 特性 | 描述 |
| :--- | :--- |
| **🤖 双重人机验证方案** | 默认采用本地题库验证，**可选** Cloudflare Turnstile 验证，配置后可在群组中**随时一键切换**，有效防止骚扰 |
| **🗑️ 双重垃圾消息拦截** | 默认采用本地规则拦截，**可选** Cloudflare Workers AI 兜底识别，可在群组中直接编辑本地规则，一键开关拦截 |
| **💬 高效私聊管理** | 使用 Telegram 群组话题功能，自动为每位私聊用户创建一个独立的话题，易于管理 |
| **⚙️ 前台设置面板** | 群组内可使用 **/settings** 打开设置面板，便于通过前台快速控制机器人开关、验证、拦截等功能 |
| **💻 管理指令系统** | 支持 **添加白名单 (/trust)**、**封禁 (/ban)**、**解封 (/unban)**、**查看黑名单 (/blacklist)** 等操作 |
| **⛔️ 严格权限管理** | 自动拦截用户发送的 `/` 管理指令，防止用户恶意骚扰。管理指令仅在群组内生效，并通过**可选**变量指定固定管理员使用 |
| **🔑 可选安全增强** | **可选**配置 Webhook Secret Token，阻止伪造请求，进一步提升安全性 |
| **☁️ 零成本部署** | 基于 Cloudflare Workers 部署，无需额外成本，高效稳定 |
| **📱 多媒体支持** | 完美支持图片、视频、文件等多种消息格式双向转发 |

---

## 💻 指令说明


| 用户指令 | 作用 |
| :--- | :--- |
| `/start` | **开始使用私聊机器人**<br>通过人机验证后可发送消息 |

| 管理员指令 | 作用 |
| :--- | :--- |
| `/help` | **显示使用说明** |
| `/trust` | **将当前用户添加白名单**<br>加入白名单的用户可以绕过垃圾消息识别，并且永不再需要进行人机验证 |
| `/ban` | **封禁用户**<br>可加用户ID，例如 /ban 或 /ban 123456<br>没用的小提示：被封禁用户向管理员发送消息将被屏蔽，但是管理员仍可对该用户单向输出 |
| `/unban` | **解封用户**<br>可加用户ID，例如 /unban 或/unban 123456 |
| `/blacklist` | **查看黑名单** |
| `/info` | **查看当前用户信息**<br>可查看用户ID、用户名、用户状态等信息 |
| `/settings` | **打开设置面板**<br>可快速控制机器人开关、人机验证、消息拦截，或使用重置机器人功能 |
| `/clean` | **删除当前话题用户的所有数据**<br>删除用户话题，清空该用户的聊天记录，并重置他的人机验证，但不会改变该用户的封禁状态或白名单状态 |

---

## 📝 前期准备

1.  **Cloudflare 账号（免费注册）**：请提前注册登录 Cloudflare 账号，并将 Cloudflare 页面设置为中文。

2.  **Telegram Bot（免费申请）**：在 [@BotFather](https://t.me/BotFather) 创建一个机器人，获取 Token。
    * ⚠️请务必保管好自己的 Token，确保不要泄露给其他人。
    * 重要设置：选择机器人，在 Bot Setting 中关闭 **Group Privacy**。

3.  **管理员群组（免费创建）**：创建一个 Telegram 群组，并 **开启话题功能 (Topics)**。
    * 将机器人拉入群组，并 **设为管理员**，务必给予管理员机器人**管理话题权限**。
    * 获取群组`SUPERGROUP_ID`，在 Telegram 桌面端右键群内任意消息，复制链接，链接里会有一段 xxxxxxxxxx 数字，在前面加上 -100 就是完整的`SUPERGROUP_ID`，例如 -100xxxxxxxxxx。

---

## 🚀 部署方法（二选一）

### 部署方法 1：GitHub 一键连接部署

这是最简单的自动化部署方式，当您更新 GitHub 仓库时，Cloudflare 会自动重新部署您的 Worker。

1.  Fork 本仓库 到您的 GitHub 账户
2.  登录 [Cloudflare Dashboard](https://dash.cloudflare.com/)
3.  选择控制台左侧 **计算和 AI** -> **Workers 和 Pages**
4.  点击页面中的 **创建应用程序**
5.  选择 **Connect Github**
6.  在弹出的页面中授权 Cloudflare 访问您的 GitHub Repository 完成仓库关联
7.  选择 **Continue with Github**，选择您刚才 Fork 的 `NextGenForward` 仓库，点击下一步
8.  **Set up your application**：
    * 项目名称：`nextgenforward` (或任意名称)
    * 其他配置保持默认
    * 点击 **部署**，等待部署完成
  
### 部署方法 2：手动复制 worker.js 代码部署

如果您不想关联 GitHub，可以直接复制本项目仓库中的 worker.js 代码部署，此方法后期将需要手动实现更新。

1.  登录 [Cloudflare Dashboard](https://dash.cloudflare.com/)
2.  选择控制台左侧 **计算和 AI** -> **Workers 和 Pages**
3.  点击页面中的 **创建应用程序**
4.  选择 **从 Hello World! 开始**
5.  编辑 **Worker name**：`nextgenforward` (或任意名称)
5.  点击 **部署**，等待部署完成
6.  点击 Cloudflare 当前 Worker 页面右上角的 **编辑代码**
7.  删除所有代码，复制并粘贴本 Github 项目仓库中 **worker.js** 文件的所有代码
8.  点击右上角 **部署**，等待页面底部提示 **版本已保存**，即为部署成功

### 部署完成后查看 Worker 域名

当您使用以上两种方法的任意一种完成部署后，您可以在 Worker 的 **概述** 页面查看您的 **Worker 域名**  
* 格式为 `your-worker-name.your-subdomain.workers.dev`  
* 例如 `nextgenforward.xxxxxx.workers.dev`

💡**您的域名之后会用到**

---

## 🔗 关联和绑定

### 必要步骤 1：创建并绑定 KV 命名空间

无论您使用以上哪种方法完成部署，都必须绑定 KV 命名空间，否则项目将无法运行。

1. 选择控制台左侧 **存储和数据库** -> **Workers KV**
2. 点击页面右上角的 **Create Instance** 创建 KV 命名空间
3. 命名空间名称填写`TOPIC_MAP`，点击 **创建** ，等待创建完成
4. 选择控制台左侧 **计算和 AI** -> **Workers 和 Pages**
5. 点击进入您刚才部署的 Worker 页面，注意 **不要点击到 Worker 域名**
6. 进入 Worker 的 **绑定** 页面 -> **添加绑定** -> 选择左侧的 **KV 命名空间** -> **添加绑定**
7. 变量名称 **必须** 填写`TOPIC_MAP`，KV 命名空间选择您刚才创建的`TOPIC_MAP`命名空间，点击 **添加绑定**
8. 绑定完成



### 必要步骤 2：添加环境变量

无论您使用以上哪种方法完成部署，都必须添加环境变量，否则项目将无法运行。

1. 选择控制台左侧 **计算和 AI** -> **Workers 和 Pages**
2. 点击进入您刚才部署的 Worker，注意 **不要点击到 Worker 域名**
3. 进入 Worker 的 **设置** 页面，找到 **变量和机密**
4. **添加** 以下环境变量，一般变量设置为 **文本**，加密变量推荐设置为 **密钥**，可选变量按需设置
   
| 必须设置的变量 | 值 | 说明 |
| :--- | :--- | :--- |
| `BOT_TOKEN` | 您的机器人Token | **加密** |
| `SUPERGROUP_ID` | -100xxxxxxxxxx |  |


| 可选变量 | 值 | 说明 |
| :--- | :--- | :--- |
| `WORKER_URL` | 您的 Worker 域名，例如`https://nextgenforward.xxxxxx.workers.dev` | **若需使用 Cloudflare 人机验证<br>则此变量必须设置** |
| `CF_TURNSTILE_SITE_KEY` | 您的 site_key（详见可选配置 1） | **若需使用 Cloudflare 人机验证<br>则此变量必须设置** |
| `CF_TURNSTILE_SECRET_KEY` | 您的 secret_key（详见可选配置 1） | **加密<br>若需使用 Cloudflare 人机验证<br>则此变量必须设置** |
| `WEBHOOK_SECRET` | 自定义随机组合 | **加密**<br>支持`A-Z` `a-z` `0-9` `_` `-`字符组合<br>使用其它字符会出错<br>设置此变量可提高 Webhook 安全性<br>**配置此变量后激活 Webhook 必须添加secret_token字段** |
| `ADMIN_IDS` | 群组管理员TG用户ID 和 您机器人的TG用户ID，例如：123456789,987654321 | 可使用 [@userinfobot](https://t.me/userinfobot) 查看<br>多个ID之间使用英文逗号连接<br>**当您群组内存在多位管理员时，您可通过此变量来控制管理员指令的使用权** |
| `API_BASE` | `https://api.telegram.org` | 缺省默认就是这个，可忽略 |



### 可选配置 1：设置 Cloudflare Turnstile

Cloudflare Turnstile 人机验证为可选配置，只有当您完成以下配置，并添加`WORKER_URL` `CF_TURNSTILE_SITE_KEY`和`CF_TURNSTILE_SECRET_KEY`**所有三个**环境变量之后才可以使用。  
机器人**默认为本地题库验证**方案，配置完成后请在群组内使用`/settings`命令通过设置面板切换为 Cloudflare 验证。

1. 首先复制您的 Worker 域名，例如 `nextgenforward.xxxxxx.workers.dev`
2. 选择控制台左侧 **应用程序安全** -> **Turnstile**
3. 点击 **添加小组件** ，设置任意小组件名称
4. 点击 **添加主机名** ，在 **添加自定义主机名** 中粘贴您的 Worker 域名（注意这里的域名**不要带`https://`开头和`/`结尾**），点击右侧 **添加**
5. 在下方 **所选主机名**中 **勾选**您刚添加的 Worker 域名，点击底部 **添加**
6. 再次在 **主机名**下勾选您刚添加的 Worker 域名，**小组件模式**选择 **托管**，**预先许可**选择**否**
7. 点击页面最右下角 **创建**
8. 创建完成后页面会显示您的 **站点密钥（site_key）**，和您的 **密钥（secret_key）**，分别对应您的`CF_TURNSTILE_SITE_KEY`和`CF_TURNSTILE_SECRET_KEY`两个变量

⚠️请务必保管好自己的密钥，确保不要泄露给其他人。


### 可选配置 2：绑定 Cloudflare Workers AI

使用 Workers AI 作为垃圾消息识别兜底，此项为可选配置。  
机器人**默认为本地规则识别方案**，当您完成以下配置，机器人会自动启用 AI 进行兜底识别，变为 **本地规则 + AI 识别兜底**。

1. 选择控制台左侧 **计算和 AI** -> **Workers 和 Pages**
2. 点击进入您刚才部署的 Worker 页面，注意 **不要点击到 Worker 域名**
3. 进入 Worker 的 **绑定** 页面 -> **添加绑定** -> 选择左侧的 **Workers AI** -> **添加绑定**
4. 变量名称 **必须** 填写`AI`，点击 **添加绑定**
5. 绑定完成

💡本项目代码中默认使用的 Workers AI 模型为`@cf/meta/llama-3.1-8b-instruct-fast`  
如果您希望更换为其他 AI 模型，或调整识别灵敏度，可手动编辑项目代码中第 202 - 206 行
**如需更换其他模型，请确保使用 Workers AI 可用的 Text Generation 模型，且必须支持 JSON Mode**
```
  ai: {
    enabled: true,
    model: "@cf/meta/llama-3.1-8b-instruct-fast",
    threshold: 0.85
  }
```
编辑完成后重新部署即可自动生效。


### 最后一步（关键）：激活 Webhook 

无论您使用以上哪种方法完成部署，都必须激活 Webhook，否则项目将无法运行。

**直接使用浏览器访问网址激活 Webhook**

**未设置`WEBHOOK_SECRET`变量：**

`https://api.telegram.org/bot<你的BOT_TOKEN>/setWebhook?url=https://<你的Worker域名>`
      
  例如：`https://api.telegram.org/bot12345678:A43eUzq3/setWebhook?url=https://nextgenforward.xxxxxx.workers.dev`

  如果页面返回 `{"ok":true, "result":true, "description":"Webhook was set"}`，即表示激活成功！
  
**已设置`WEBHOOK_SECRET`变量：**

`https://api.telegram.org/bot<你的BOT_TOKEN>/setWebhook?url=https://<你的worker域名>/&secret_token=<你的WEBHOOK_SECRET>`
      
  例如：`https://api.telegram.org/bot12345678:A43eUzq3/setWebhook?url=https://nextgenforward.xxxxxx.workers.dev/&secret_token=Abc_1234-5d6F7G`

  如果页面返回 `{"ok":true, "result":true, "description":"Webhook was set"}`，即表示激活成功！


### 🎉 大功告成！至此您已完成项目的所有部署和配置，可以正常开始使用！

💡 此时建议您访问一下自己的 Worker 域名，如果页面返回 OK，则表示已经部署成功，如果报错则说明您的部署过程出现问题。

---

## ⚙️ 一些额外的配置（仅了解即可）

**在 worker.js 文件开头包含一些可调配置常量，可按需手动更改，重新部署后生效，实现部分实用功能调节**  

    // 用户速率限制
    RATE_LIMIT_VERIFY: 3,              // 用户5分钟内最多可尝试人机验证次数，不可设为0
    RATE_LIMIT_MESSAGE: 45,            // 用户私聊消息发送速率限制，不可设为0
    RATE_LIMIT_WINDOW: 60,             // 用户私聊消息速率限制窗口，不可设为0
    
    // 人机验证配置
    VERIFY_BUTTON_TEXT: "🤖 点击进行人机验证",     // 人机验证按钮文本
    PENDING_MAX_MESSAGES: 10,          // 人机验证期间最多暂存消息数量，不可设为0

---

## ❓ 常见问题及解决方法

**Q: 为什么机器人没反应？**  
A: 您的环境变量设置有误 / 未绑定 KV 命名空间 / 未设置 Webhook，未在 Bot Setting 中关闭 Group Privacy，请检查。

**Q: 为什么机器人不创建话题？**  
A: 请确保您的群组已开启话题（Topics）功能，并且已给予机器人管理员权限，开启了机器人的管理话题权限，`SUPERGROUP_ID`环境变量已正确设置。

**Q: 为什么群组内不显示指令菜单？**  
A: 请访问一下您的 Worker 域名，若页面返回 OK，此时返回您的 Telegram 首页，再次打开群组就可以刷新出来了。

**Q: 为什么我将用户封禁后立刻查看黑名单无法找到他？**  
A: 这是由于 Cloudflare 边缘节点最终一致性所导致的延迟问题，一般等待几秒后即可正常刷新。

**Q: 我误用 /trust 命令将错误的人加进了白名单，怎样移除？**  
A: /ban 他。

**Q: 为什么用户打开人机验证页面报错 Worker Origin Error？**  
A: 请确保您的`WORKER_URL` `CF_TURNSTILE_SITE_KEY`和`CF_TURNSTILE_SECRET_KEY`**所有三个**环境变量均已在 CF 控制台设置，一般这个报错是您未设置`WORKER_URL`所导致的。

**Q: 怎样取消激活 Webhook？**  
A: `https://api.telegram.org/bot<BOT_TOKEN>/deleteWebhook?drop_pending_updates=true`



**如果您喜欢这个项目，还请 Star ⭐️**
