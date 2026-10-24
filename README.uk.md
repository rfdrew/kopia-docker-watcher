<!-- translated-from: README.md @ d509c06f4f196a937bfeeb3284a671d52c490a3b -->
[English](README.md) | Українська

# kopia-docker-watcher

kopia-docker-watcher керує резервним копіюванням [Kopia](https://kopia.io) за мітками контейнерів Docker. Додайте
до контейнера `kopia.enable=true` і `kopia.path=/srv/app/data`, і watcher зареєструє цей шлях як джерело знімків
у вашому kopia server, підтримуватиме його розклад відповідно до міток, ставитиме його на паузу, поки контейнер
зупинено, і робитиме знімки під час запуску чи зупинки контейнера.

Це наступник програми на Python `kopia-docker-operator`, написаний на Go. Мітки сумісні; розгортання самого watcher —
ні. Див. [docs/uk/migration.md](docs/uk/migration.md).

## <a id="how-it-works"></a>Як це працює

```
 Docker Engine                 kopia-docker-watcher                     kopia server
 ─────────────                 ────────────────────                     ────────────
 container events  ──────────▶  queue of source paths  ──── REST API ──▶  policies (schedule, pause)
 (start, die, destroy, …)       one reconcile pass per path              sources/upload, snapshots/edit
 container list    ◀──────────  full reconcile every RECONCILE_INTERVAL  snapshots/delete, policy/resolve
 exec (hooks)      ◀──────────  STATE_FILE: what was applied, owners,
                                the schedule of sources with hooks
```

- Watcher спілкується з kopia server лише через його HTTP API — той самий, яким користується вебінтерфейс Kopia.
  Він ніколи не запускає CLI `kopia`, не відкриває репозиторій і не читає ваших файлів: **їх читає kopia server**,
  тож кожен шлях у `kopia.path` має бути видимим усередині контейнера kopia server.
- Подія Docker лише повідомляє watcher, на який контейнер подивитися. Кожен прохід читає поточний стан усіх
  контейнерів і узгоджує джерело одного шляху з мітками кожного контейнера, що вказує цей шлях, зокрема й
  зупинених. Повне звіряння кожні `RECONCILE_INTERVAL` (типово 5 хвилин) підхоплює все, що пропустила подія.
- Політика шляху об'єднується, а не замінюється: watcher записує лише ті поля, які задають його мітки (розклад, а
  також зберігання, стиснення, правила ігнорування та інші поля міток політики), і паузу. Усе інше, що ви
  налаштували у вебінтерфейсі Kopia, лишається як є, а поле, мітку якого видалено, знову успадковується.
- Те, що watcher застосував, знімки, на які він чекає, і те, які контейнери володіють кожним джерелом, зберігається
  у `STATE_FILE` — невеликому JSON-файлі, який має лежати на томі.

## <a id="what-it-does"></a>Що він робить

- Реєструє джерело для кожного шляху з `kopia.path` і підтримує `scheduling.cron` відповідно до `kopia.schedule`.
- Знаходить через Docker шляхи на хості для томів і bind-монтувань контейнера за `kopia.volumes` (`data`, точка
  монтування на кшталт `/var/lib/postgresql/data` або `*`), пропускаючи `kopia.exclude-volumes`. Див.
  [docs/uk/labels.md](docs/uk/labels.md#kopiavolumes).
- Задає поля політики шляху, яких вимагають [мітки політики](docs/uk/labels.md#policy-labels): інтервал або час
  доби, зберігання, правила ігнорування й обмеження на файли, обробку помилок, стиснення (перевіряється за
  алгоритмами, які є в kopia server), паралельне читання файлів і будь-яке інше поле через `kopia.policy-json`.
- Ставить джерело на паузу (`scheduling.manual=true`), поки всі його контейнери зупинено, і відновлює попередній
  розклад, коли один із них знову запускається. Контейнер, що циклічно падає, не перемикає паузу.
- Робить знімок під час запуску контейнера (`kopia.snapshot-on-start`, не частіше, ніж дозволяє
  `kopia.min-interval`) і під час зупинки (`kopia.snapshot-on-stop`), стежить за ним до кінця і записує результат
  у лог.
- Закріплює знімок під час зупинки за допомогою `kopia.pin-on-stop` (наприклад, `before-update-{date}`), щоб
  правила зберігання зберегли стан до оновлення; очищення також зберігає закріплені знімки.
- Виконує [хуки](docs/uk/labels.md#hooks) у контейнері навколо його знімків: `kopia.pre-exec` до знімка (скажімо, дамп
  бази даних; якщо він завершується помилкою, знімок не робиться) і `kopia.post-exec` після, хоч би чим закінчився
  знімок, кожен у межах `kopia.hook-timeout`. kopia server робив би заплановані знімки без хуків, тому watcher сам
  робить заплановані знімки такого джерела в моменти, які дає політика kopia server, з урахуванням `runMissed`, і
  тримає розклад kopia server для нього вимкненим.
- [Зупиняє або призупиняє](docs/uk/labels.md#stopping-and-pausing) контейнер на час його знімків
  (`kopia.stop-during-backup`, `kopia.pause-during-backup`) разом із контейнерами, що від нього залежать
  (`kopia.depends-on`), і запускає його знову після цього, найпізніше через `kopia.max-downtime`, а також коли
  watcher зупиняється; контейнер, який watcher залишив вимкненим під час свого збою, повертається, коли watcher
  знову запускається.
- Записує `kopia.description` і `kopia.tags` в опис знімка — як для власних знімків, так і для запланованих.
- Кілька контейнерів можуть спільно використовувати шлях: джерело ставиться на паузу, лише коли всі вони зупинені.
- Очищає джерела видалених контейнерів так, як вимагає `kopia.cleanup-on-remove`, після `CLEANUP_GRACE`: пауза
  (типово), видалення політики джерела, яке не має знімків, або видалення джерела разом зі знімками (закріплені
  лишаються, якщо не задано `kopia.cleanup-delete-pinned=true`). Коли контейнери спільно використовують шлях,
  застосовується найобережніший режим серед них.
- Перевіряє сертифікат TLS kopia server за його відбитком SHA-256, файлом CA або системними кореневими
  сертифікатами.
- `validate` перевіряє мітки, перш ніж ви їх розгорнете; `run --dry-run` показує, що зробив би watcher, нічого не
  змінюючи.
- `list`, `trigger` і `restore`, запущені через `docker exec` у контейнері watcher, звертаються до запущеного
  watcher через його [керувальний сокет](docs/uk/configuration.md#control_socket): `list` показує керовані джерела та
  їхній стан, `trigger` робить знімок через чергу watcher із хуками, простоєм і мінімальним інтервалом із міток, а
  `restore` відновлює знімок у каталог на kopia server. Див. [docs/uk/restore.md](docs/uk/restore.md).
- Віддає метрики Prometheus на `/metrics`: коли кожне джерело востаннє мало повний знімок (зокрема й
  запланований), збої та тривалість власних знімків watcher і хуків, помилки звіряння, а також керовані джерела з
  їхніми контейнерами. Див. [docs/uk/metrics.md](docs/uk/metrics.md).
- Перевіряє на `/healthz`, що Docker відповідає, що kopia server відповідає і має під'єднаний репозиторій і що цикл
  звіряння не завис, і відповідає 503, коли щось із цього не працює вже деякий час; його використовує `HEALTHCHECK`
  образу. Див. [перевірку стану](docs/uk/configuration.md#health-check).
- Надсилає сповіщення на `NOTIFY_WEBHOOK_URL`, коли шлях починає давати збої (невдалий знімок watcher, невдалий
  хук, мітки, які неможливо застосувати, kopia server, що не працює вже деякий час), і коли він знову працює —
  по одному разу, у форматі JSON або для ntfy, Slack чи Telegram. Див. [сповіщення](docs/uk/configuration.md#notifications).
- Задає обмеження kopia server на вивантаження, завантаження та операції зі сховищем зі змінних `KOPIA_THROTTLE_`;
  якщо їх не задано, залишає їх на розсуд kopia server або Terraform. Див. [обмеження швидкості](docs/uk/configuration.md#throttle).
  [docs/uk/terraform.md](docs/uk/terraform.md) розподіляє конфігурацію kopia server між watcher і
  terraform-provider-kopia.

Поки що не підтримується:

- Немає тегів знімків: kopia server поки не вміє задавати їх через свій API, тому `kopia.tags` потрапляють в опис.

## <a id="requirements"></a>Вимоги

**Docker.** Будь-який Docker Engine з API 1.40 (Docker 19.03) або новіший. Watcher потрібен доступ на читання до
списку контейнерів, інспектування контейнерів і потоку подій: через `/var/run/docker.sock` або через
[docker-socket-proxy](https://github.com/Tecnativa/docker-socket-proxy), що дозволяє `CONTAINERS` і `EVENTS`
(див. [examples/docker-compose.yml](examples/docker-compose.yml)). Перевірка стану пінгує демон (`/_ping`), і
docker-socket-proxy це типово дозволяє. [Хуки](docs/uk/labels.md#hooks) виконуються через `docker exec`, якому з
docker-socket-proxy потрібні ще й `EXEC=1` та `POST=1`, а [зупинці та
призупиненню](docs/uk/labels.md#stopping-and-pausing) контейнерів потрібен `POST=1` (див.
[Docker](docs/uk/configuration.md#docker)).

**kopia server.** Watcher тестується з останнім релізом Kopia, 0.23.1. kopia server має працювати за таких умов:

- **вебінтерфейс увімкнено**, тобто без `--no-ui`. Watcher використовує API вебінтерфейсу (`/api/v1/…`), який
  `--no-ui` вимикає разом із вебінтерфейсом. Тоді watcher зупиняється з повідомленням «kopia server does not serve
  its UI API».
- **облікові дані watcher — це користувач сервера**: `KOPIA_SERVER_USERNAME` і `KOPIA_SERVER_PASSWORD` мають
  збігатися з `--server-username` і `--server-password` kopia server. Користувачі, додані через
  `kopia server user add`, — це користувачі репозиторію, і вони не можуть використовувати API вебінтерфейсу; kopia
  server відповідає їм 403. Без `--server-username` користувач kopia server — `kopia`, і це також типове значення
  watcher, коли задано лише `KOPIA_SERVER_PASSWORD`.
- **`--disable-csrf-token-checks` не потрібен.** Watcher отримує токен CSRF так само, як вебінтерфейс, і отримує
  новий після перезапуску kopia server, тож перевірки CSRF можна залишити ввімкненими.
- **шляхи видимі.** Кожен шлях у `kopia.path` має існувати всередині контейнера kopia server — за тим самим шляхом
  або під `HOST_PREFIX` (див. [HOST_PREFIX](docs/uk/configuration.md#host_prefix)). kopia server перевіряє, що шлях
  існує, коли watcher його реєструє. Для `kopia.volumes` kopia server має бачити каталог томів Docker, типово
  `/var/lib/docker/volumes`, і шляхи bind-монтувань на хості: змонтуйте їх лише для читання, наприклад
  `/var/lib/docker/volumes:/var/lib/docker/volumes:ro`.
- **стабільне ім'я хоста.** kopia server реєструє джерела як `user@host` свого під'єднання до репозиторію. Задайте
  `hostname:` для контейнера kopia server або створіть репозиторій із `--override-hostname`, щоб контейнер,
  створений заново, не реєстрував кожен шлях повторно під новим іменем.
- **сертифікат TLS, що зберігається** між перезапусками, якщо ви використовуєте HTTPS. З `--tls-generate-cert` і
  без файлів сертифіката kopia server створює новий сертифікат, а отже, і новий відбиток, під час кожного запуску.
  Згенеруйте його один раз у `--tls-cert-file` і `--tls-key-file` на томі, а потім запускайте kopia server із цими
  файлами й без `--tls-generate-cert` (kopia server відмовляється перезаписувати наявні файли).

**Відбиток.** Коли kopia server генерує свій сертифікат, він записує в лог

```
SERVER CERT SHA256: 3f0c9d…(64 hex digits)
```

Вставте це шістнадцяткове значення в `KOPIA_SERVER_CERT_FINGERPRINT`. Двокрапки й великі літери допускаються, тож
для наявного сертифіката підійде й значення після `Fingerprint=` у виведенні
`openssl x509 -in tls.crt -noout -fingerprint -sha256`. Формат `SHA256:…`,
який радив README версії на Python, відхиляється з підказкою. Якщо відбиток не збігається, watcher зупиняється і
записує в лог відбиток, який надав сервер.

## <a id="quick-start"></a>Швидкий старт

Приклад в [examples/](examples/) запускає kopia server, watcher і зразковий контейнер на одному хості.

```bash
cd examples
cp .env.example .env
# Set the passwords in .env, and DOCKER_GID to the group of the Docker socket:
stat -c %g /var/run/docker.sock
docker compose up -d kopia
docker compose logs kopia | grep 'SERVER CERT SHA256'
# Put the hex value into KOPIA_SERVER_CERT_FINGERPRINT in .env, then:
docker compose up -d
docker compose logs -f watcher
```

Потім відкрийте вебінтерфейс Kopia за адресою `https://localhost:51515` (браузер попередить про самопідписаний
сертифікат) і знайдіть джерело `/srv/kopia-docker-watcher-example/data` з його першим знімком.

Перш ніж додавати мітки до власних контейнерів, перевірте їх:

```bash
docker compose exec watcher kopia-docker-watcher validate --all
docker compose run --rm watcher run --dry-run
```

## <a id="commands"></a>Команди

Образ типово запускає `kopia-docker-watcher run`.

| Команда | Що вона робить |
|---|---|
| `run [--strict]` | Запускає watcher: чекає, доки kopia server матиме під'єднаний репозиторій, а потім звіряє після кожної події контейнера і кожні `RECONCILE_INTERVAL`. Зупиняється, отримавши SIGTERM або SIGINT. |
| `run --dry-run [--strict]` | Один раз звіряє кожен шлях і показує зміни політик, час наступних запланованих знімків і очищення, яке було б виконано. Нічого не записує в kopia server чи `STATE_FILE`, не обслуговує HTTP і може працювати поруч із запущеним watcher. kopia server уже має мати під'єднаний репозиторій: пробний запуск на нього не чекає. Завершується з кодом 1, якщо знайшов проблему. |
| `validate [--strict] label=value… \| -` | Перевіряє мітки, передані аргументами, або по одній на рядок зі stdin із `-`. |
| `validate [--strict] --container NAME…` | Перевіряє мітки контейнерів. |
| `validate [--strict] --all` | Перевіряє кожен контейнер із `kopia.enable`, зокрема й зупинені. |
| `list [--json]` | Показує джерела, якими керує запущений watcher: шлях, контейнери, чи стоїть на паузі, хто планує знімки і коли наступний, останній повний знімок, стан у kopia server та останню проблему. |
| `trigger [--force] [--no-wait] CONTAINER\|PATH` | Робить знімок кожного шляху контейнера або одного шляху через чергу запущеного watcher, із хуками, зупинкою/призупиненням і `kopia.min-interval`, і чекає на його результат. `--force` ігнорує `kopia.min-interval`; `--no-wait` повертає керування, щойно знімок запитано. |
| `restore --target DIR [--overwrite] [--stop] CONTAINER\|PATH [SNAPSHOT]` | Відновлює знімок, типово найновіший повний, у `DIR` на kopia server через запущений watcher (`POST /api/v1/restore`) і стежить за завданням kopia server до кінця. Відмовляється працювати з непорожнім `DIR`, якщо не задано `--overwrite`; `--stop` зупиняє контейнери шляху на час виконання. Див. [docs/uk/restore.md](docs/uk/restore.md). |
| `healthcheck` | Завершується з кодом 0, якщо watcher у тому самому контейнері відповідає 200 на `/healthz`, тобто не перебуває в стані unhealthy; його використовує `HEALTHCHECK` образу. |
| `version` | Показує версію. |

`--strict` перетворює кожне попередження про мітки на помилку. Без `KOPIA_SERVER_URL` команда `validate` перевіряє
лише мітки; із цією змінною `validate` потребує повної конфігурації watcher (облікові дані, TLS) і kopia server із
під'єднаним репозиторієм, а kopia server ще й нормалізує кожен шлях, обчислює час наступних знімків за розкладом,
що відхиляє хибний вираз cron, і перевіряє алгоритм стиснення. Жоден із двох режимів не перевіряє, що шлях існує.
`validate` завершується з кодом 0, якщо мітки можна застосувати (попередження допускаються), 1 — якщо ні або якщо
не вдалося звернутися до Docker чи kopia server, і 2 — у разі помилки використання чи конфігурації. `run --help` і
`validate --help` показують повну довідку з використання та коди виходу.

`list`, `trigger` і `restore` спілкуються з watcher, запущеним у тому самому контейнері, через unix-сокет
`CONTROL_SOCKET`, тож запускайте їх через `docker exec`:

```bash
docker compose exec watcher kopia-docker-watcher list
docker compose exec watcher kopia-docker-watcher trigger app
docker compose exec watcher kopia-docker-watcher restore --target /restore/app app
```

Коли watcher там не запущено, вони завершуються помилкою і повідомляють про це; самостійно вони ніколи не діють на
kopia server чи Docker. Кожна з них завершується з кодом 0 у разі успіху, 1 — коли сталася помилка або до watcher
неможливо звернутися, і 2 — у разі помилки використання; `--help` показує повну довідку з використання. На
`LISTEN_ADDR` не обслуговується нічого, що керує watcher.

Логи — це рядки JSON у stderr.

## <a id="documentation"></a>Документація

- [docs/uk/configuration.md](docs/uk/configuration.md): змінні середовища.
- [docs/uk/labels.md](docs/uk/labels.md): мітки контейнерів.
- [docs/uk/metrics.md](docs/uk/metrics.md): метрики Prometheus, конфігурація збирання метрик і приклади алертів.
- [docs/uk/restore.md](docs/uk/restore.md): `list`, `trigger` і `restore` з прикладами та тим, на що варто зважати.
- [docs/uk/terraform.md](docs/uk/terraform.md): робота з terraform-provider-kopia, хто чим володіє.
- [docs/uk/migration.md](docs/uk/migration.md): перехід із програми на Python `kopia-docker-operator`.
- [examples/](examples/): файл compose із kopia server, watcher і контейнером із мітками.

Це український переклад документації; першоджерело — англійський текст (`README.md` і `docs/en/`). Змінюючи
англійський текст, оновіть його український відповідник у тому самому pull request, використовуючи терміни з
[docs/uk/GLOSSARY.md](docs/uk/GLOSSARY.md), або напишіть у pull request, що переклад відстає. `make docs-check`
показує відсутні й застарілі переклади та биті посилання.

## <a id="image"></a>Образ

Релізи публікують `ghcr.io/rfdrew/kopia-docker-watcher` для linux/amd64 і linux/arm64 з тегами: версія (`0.1.0`),
`major.minor` (`0.1`) і `latest`; передрелізи отримують лише власний тег. Образ побудовано на distroless, і він
працює від імені некореневого користувача 65532, якому потрібен доступ до сокета Docker (див. приклад).

## <a id="license"></a>Ліцензія

[Apache License 2.0](LICENSE).
