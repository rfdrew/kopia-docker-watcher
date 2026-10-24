<!-- translated-from: docs/terraform.md @ d697bf00e12a9b05382727b0e42110a6222e7f12 -->
[English](../terraform.md) | Українська

# <a id="with-the-terraform-provider"></a>З провайдером Terraform

[terraform-provider-kopia](https://github.com/rfdrew/terraform-provider-kopia) (`rfdrew/kopia`) керує
конфігурацією kopia server як кодом через той самий HTTP API, яким користується watcher. Обидва працюють поруч,
коли кожен володіє своєю частиною конфігурації kopia server: Terraform — тим, що задається один раз на сервер
або хост, watcher — тим, що змінюється разом із контейнерами.

## <a id="who-owns-what"></a>Хто чим володіє

| Конфігурація | Власник | Чому |
|---|---|---|
| Глобальна політика, `(global)` | Terraform | Зберігання, стиснення, правила ігнорування, обробка помилок: типові значення, які успадковує кожне джерело. |
| Політики хоста й користувача, `@host` і `user@host` | Terraform | Типові значення для всіх джерел одного хоста, як-от розклад для джерел, що не задають власного. |
| Профілі сповіщень | Terraform | Власні сповіщення kopia server про знімки, які він виконує: заплановані й ті, яких просить watcher. |
| Обмеження швидкості (`/api/v1/repo/throttle`) | Terraform | Один набір обмежень на сервер. Не задавайте змінні `KOPIA_THROTTLE_` у watcher. |
| Політики шляхів контейнерів, `user@host:/path` | Watcher | Їхні розклад і паузу визначають мітки та стан контейнерів. |
| Знімки watcher, їхні описи, очищення видалених контейнерів | Watcher | Їх визначають події контейнерів. |
| Сповіщення про власні знімки й мітки watcher | Watcher, `NOTIFY_WEBHOOK_URL` | kopia server нічого не знає про мітки; див. [configuration.md](configuration.md#notifications). |

Правила, на яких ґрунтується таблиця:

- **Ніколи не описуйте в Terraform політику шляху, якою керує watcher.** Провайдер володіє всією політикою
  цілі й замінює її під час кожного apply, тоді як watcher записує в неї поля своїх міток: кожен скасовував би
  зміни іншого під час кожного apply і кожного повного звіряння. Натомість винесіть спільне для всіх джерел у
  глобальну політику або політику хоста; політики шляхів успадковують кожне поле, яке не задають самі.
- **Watcher ніколи не записує глобальну політику чи політику хоста.** Він лише читає чинну політику своїх
  шляхів, щоб перевірити розклад, перш ніж його застосувати. У політиці шляху він записує лише поля, які задають
  його мітки, і паузу, а все інше зберігає, тож поле, задане для шляху у вебінтерфейсі Kopia, лишається.
- **Один власник обмеження швидкості.** Watcher задає обмеження швидкості лише тоді, коли задано змінну
  `KOPIA_THROTTLE_`, і тоді — під час кожного запуску. Якщо ним керує Terraform, не задавайте жодної з них.
  kopia server зберігає обмеження в конфігурації свого репозиторію, тож вони так чи так переживають його
  перезапуски.
- **Розділяйте шляхи за допомогою `WATCHER_PATH_PREFIX`**, якщо Terraform також керує деякими політиками
  шляхів, наприклад каталогів поза Docker: watcher ніколи не реєструє, не ставить на паузу й не видаляє джерело
  поза префіксом ([configuration.md](configuration.md#watcher_path_prefix)).

## <a id="notifications-from-both"></a>Сповіщення від обох

Профілі сповіщень kopia server повідомляють про знімки, які виконує kopia server: ті, що за розкладом політик,
і ті, яких watcher просить під час запуску чи зупинки контейнера. `NOTIFY_WEBHOOK_URL` у watcher повідомляє
про те, чого kopia server не може: про знімок watcher, кінець якого загубився, як-от коли kopia server
перезапустився, про мітки, які kopia server відхиляє, і про збої kopia server для шляху, що тривають
певний час після запуску watcher. Він також повідомляє про невдалий знімок watcher — один раз для кожного шляху, доки
знімок знову не вдасться. Обидва можуть надсилати в ту саму тему ntfy чи той самий канал Slack.

## <a id="credentials"></a>Облікові дані

Watcher і провайдер входять однаково — як користувач вебінтерфейсу kopia server (`--server-username` і
`--server-password`) — і читають ті самі змінні середовища: `KOPIA_SERVER_URL`, `KOPIA_SERVER_USERNAME`,
`KOPIA_SERVER_PASSWORD`, `KOPIA_SERVER_CERT_FINGERPRINT`, `KOPIA_SERVER_CA_FILE` і
`KOPIA_SERVER_INSECURE_SKIP_VERIFY`. Один `.env` може слугувати обом; відрізнятися може лише `KOPIA_SERVER_URL`,
коли Terraform звертається до kopia server ззовні мережі Docker.

Блок провайдера, як у власному прикладі провайдера:

```hcl
terraform {
  required_providers {
    kopia = {
      source  = "rfdrew/kopia"
      version = "~> 0.1"
    }
  }
}

# Credentials are best supplied through the environment so they stay out of the
# configuration: KOPIA_SERVER_URL, KOPIA_SERVER_USERNAME and
# KOPIA_SERVER_PASSWORD.
provider "kopia" {
  url      = "https://kopia.example.com:51515"
  username = "kopia"
  password = var.kopia_password

  # The server generated its own certificate (`kopia server start
  # --tls-generate-cert`) and printed its fingerprint as "SERVER CERT SHA256".
  tls_cert_fingerprint = "cd50843049f0c6da48255e8022728d28124cb5b72fc3a2d81be496219c2cccf3"
}

variable "kopia_password" {
  description = "Password of the kopia server's UI user, as given to `kopia server start --server-password`."
  type        = string
  sensitive   = true
}
```

## <a id="resources"></a>Ресурси

Документація провайдера анонсує `kopia_policy` для політик, `kopia_notification_profile` і `kopia_throttle`,
але його перший реліз, 0.1.0, ще не вийшов і поки що не має ресурсів. Коли їх випустять, ця сторінка отримає
приклад для кожної частини таблиці вище; доти задавайте глобальну політику й політики хостів, профілі сповіщень
і обмеження швидкості у вебінтерфейсі Kopia або за допомогою командного рядка `kopia` і дотримуйтеся того самого розподілу.
