<!-- translated-from: docs/metrics.md @ d697bf00e12a9b05382727b0e42110a6222e7f12 -->
[English](../metrics.md) | Українська

# <a id="metrics"></a>Метрики

Watcher віддає метрики [Prometheus](https://prometheus.io) за адресою `GET /metrics` на
[`LISTEN_ADDR`](configuration.md#listen_addr) (типово `:8080`), поруч із `/healthz`, у текстовому форматі
Prometheus. `run --dry-run` не обслуговує HTTP, а отже, й метрик не віддає. `/healthz` описано в
[configuration.md](configuration.md#health-check).

Ендпоінт не має ні автентифікації, ні TLS, а метрики містять шляхи ваших джерел і назви контейнерів. Тримайте
`LISTEN_ADDR` у мережі, якій ви довіряєте: збирайте метрики з тієї мережі Docker, у якій працює watcher, а не
публікуйте порт.

## <a id="summary"></a>Зведення

| Метрика | Тип | Мітки | Що показує |
|---|---|---|---|
| [`kopia_watcher_last_success_timestamp_seconds`](#kopia_watcher_last_success_timestamp_seconds) | gauge | `path` | Початок останнього повного знімка джерела, зокрема запланованого |
| [`kopia_watcher_snapshot_duration_seconds`](#kopia_watcher_snapshot_duration_seconds) | histogram | — | Скільки тривали власні знімки watcher |
| [`kopia_watcher_snapshot_failures_total`](#kopia_watcher_snapshot_failures_total) | counter | `path` | Власні знімки watcher, які не зберегли повного знімка |
| [`kopia_watcher_reconcile_errors_total`](#kopia_watcher_reconcile_errors_total) | counter | `kind` | Проблеми механізму звіряння |
| [`kopia_watcher_managed_sources`](#kopia_watcher_managed_sources-and-kopia_watcher_source_paused) | gauge | `state` | Керовані джерела, активні й на паузі |
| [`kopia_watcher_source_paused`](#kopia_watcher_managed_sources-and-kopia_watcher_source_paused) | gauge | `path` | 1, поки джерело на паузі, 0, поки воно активне |
| [`kopia_watcher_source_owner`](#kopia_watcher_source_owner) | gauge | `path`, `container` | 1 для кожного контейнера, який робить резервну копію джерела |
| [`kopia_watcher_hook_failures_total`](#kopia_watcher_hook_failures_total) | counter | `path`, `hook` | Команди хуків, що завершилися помилкою |
| [`kopia_watcher_hook_duration_seconds`](#kopia_watcher_hook_duration_seconds) | histogram | `hook` | Скільки тривали команди хуків |
| [`kopia_watcher_backup_downtime_seconds`](#kopia_watcher_backup_downtime_seconds) | histogram | `mode` | Скільки тривав простій контейнерів, зупинених чи призупинених для знімка |
| [`kopia_watcher_return_failures_total`](#kopia_watcher_return_failures_total) | counter | `path`, `container` | Невдалі спроби знову запустити чи відновити такий контейнер |
| [`kopia_watcher_notifications_total`](#kopia_watcher_notifications_total) | counter | `result` | Сповіщення на `NOTIFY_WEBHOOK_URL`: надіслані, невдалі, відкинуті |
| `kopia_watcher_build_info` | gauge | `version` | Завжди 1; версія watcher |

Разом із ними віддаються метрики середовища виконання Go (`go_*`) і процесу (`process_*`).

Мітки містять лише шляхи джерел і назви контейнерів, ніколи не ID завдань, знімків чи контейнерів, тож
кількість рядів зростає разом із кількістю ваших джерел і контейнерів і більше ні від чого не залежить.

## <a id="sources"></a>Джерела

Джерело вважається керованим, щойно прохід звіряння узгодив його політику з мітками його
контейнерів. Перше повне звіряння, одразу після того, як watcher під'єднався до kopia server, робить це для
кожного джерела, тож ряди джерел з'являються незабаром після запуску watcher, зокрема й після перезапуску,
після якого лічильники знову починаються з 0.

`path` — це шлях джерела так, як його бачить kopia server, після [`HOST_PREFIX`](configuration.md#host_prefix):
той шлях, який показують логи й вебінтерфейс Kopia. Джерела іншої ідентичності kopia server у `STATE_FILE`, а
також джерела поза [`WATCHER_PATH_PREFIX`](configuration.md#watcher_path_prefix), не керуються й рядів не мають.

Коли watcher перестає керувати джерелом, його ряди зникають разом із тим проходом по його шляху, який це
виявляє, а не лишаються з останнім значенням: коли `kopia.cleanup-on-remove` видалила джерело, kopia server
більше не знає його чи відмовився його зареєструвати, або `WATCHER_PATH_PREFIX` більше його не охоплює. Прохід,
що завершується помилкою, перш ніж зможе це визначити, скажімо, поки kopia server недоступний, лишає ряди без змін.

## <a id="metrics-1"></a>Метрики

### `kopia_watcher_last_success_timestamp_seconds`

Час початку останнього повного знімка джерела, у секундах від початку епохи Unix, за годинником kopia server.
Повний означає, що kopia server зберіг знімок без причини неповноти: контрольна точка вивантаження, яке ще
триває, чи скасований знімок не враховуються, а знімок, який kopia server зберіг повним, але з помилками в
деяких файлах, враховується.

Враховується кожен знімок джерела, а не лише власні знімки watcher: більшість із них kopia server робить за
розкладом, і знімки, зроблені у вебінтерфейсі чи через kopia CLI, теж враховуються. Watcher читає значення зі
стану джерела в kopia server під час кожного проходу по його шляху, тобто щонайменше кожні
[`RECONCILE_INTERVAL`](configuration.md#reconcile_interval), а також одразу після завершення кожного власного
знімка. Коли останній знімок — контрольна точка чи неповний з іншої причини, watcher переглядає список знімків
джерела, щоб знайти останній повний, один раз для кожного такого останнього знімка.

Джерело без повного знімка, як-от щойно зареєстроване, ряду не має.

### `kopia_watcher_snapshot_duration_seconds`

Скільки тривали знімки, які watcher поставив у чергу: під час запуску чи зупинки контейнера, під час запуску
watcher із [`SNAPSHOT_ON_WATCHER_START`](configuration.md#snapshot_on_watcher_start), за власним розкладом watcher
для джерела з [хуками](labels.md#hooks) або повторно після того, як kopia server втратив запуск. Це час від
початку до кінця завдання знімка в kopia server, тож знімок, який чекав на слот вивантаження, час очікування не
враховує.

Враховуються лише знімки, що дійшли до кінця: ті, що зберегли знімок, повний, неповний чи з помилками, і ті, що
не зберегли жодного, бо нічого не змінилося (з `ignoreIdenticalSnapshots` у політиці). Невдалі й скасовані знімки
та втрачені запуски враховуються лише в `kopia_watcher_snapshot_failures_total`. Знімки, які kopia server робить
за розкладом, не враховуються: watcher за ними не стежить. Заплановані знімки джерела з хуками враховуються:
їх робить watcher.

Кошики гістограми: 1 с, 5 с, 15 с, 30 с, 1 хв, 5 хв, 15 хв, 30 хв, 1 год, 2 год, 4 год і 8 год. Мітки `path`
немає, щоб кількість рядів лишалася малою; список завдань kopia server показує час кожного знімка.

### `kopia_watcher_snapshot_failures_total`

Знімки джерела, які watcher поставив у чергу і які не зберегли повного знімка:

- завдання завершилося помилкою;
- вивантаження скасовано, у вебінтерфейсі чи через паузу;
- знімок неповний або має помилки, які політика не ігнорує;
- kopia server втратив запуск, як-от під час свого перезапуску, і watcher не знаходить його знімка. Тоді watcher
  робить знімок повторно, і цей запуск враховується ще раз, якщо теж завершиться помилкою. Запуск, який kopia
  server лише перестав відстежувати, не враховується: його завдання може зникнути зі списку останніх 50 завдань
  kopia server раніше, ніж watcher побачить його завершення, скажімо, поки watcher перезапускається, і тоді
  watcher знаходить знімок цього запуску.

Збій враховується, коли знімок завершується, зокрема й тоді, коли контейнера, що його запросив, уже немає. Ряд
джерела починається з 0, коли джерело стає керованим, щоб `increase()` побачила його перший збій. Виняток —
збій до першого збирання метрик нового джерела: Prometheus уперше бачить ряд зі значенням 1, тому алерт нижче
також шукає нові ряди зі значенням понад 0.

Знімки, які kopia server робить за розкладом, не враховуються. Про їхні збої kopia server повідомляє у своєму
списку завдань і своїх профілях сповіщень; у метриках відстає `kopia_watcher_last_success_timestamp_seconds`.
Заплановані знімки джерела з [хуками](labels.md#hooks) враховуються, бо їх робить watcher. Знімок із хуками, у
якого `kopia.pre-exec` завершився помилкою, знімка не робить і натомість враховується в
`kopia_watcher_hook_failures_total`.

### `kopia_watcher_reconcile_errors_total`

Проблеми, на які натрапив механізм звіряння, за `kind`. Кожен вид є від самого початку, зі значенням 0.

| `kind` | Що пішло не так | Як враховується |
|---|---|---|
| `unavailable` | kopia server недосяжний або не має під'єднаного репозиторію | під час кожної спроби; спроба для шляху повторюється з дедалі більшою затримкою |
| `temporary` | kopia server повернув помилку, після якої спроба повторюється, або змінилася його ідентичність | під час кожної спроби |
| `docker` | не вдалося отримати список контейнерів | під час кожної спроби, зокрема спроби повного звіряння |
| `config` | kopia server відхиляє розклад, політику чи шлях, яких вимагають мітки | один раз, коли проблема виникає, і знову під час кожного повного звіряння, поки вона триває |
| `labels` | мітки контейнера мають помилки | один раз для контейнера й набору міток; для джерела запущеного контейнера також так само, як `config` |
| `owners_disagree` | запущені контейнери зі спільним шляхом вимагають різних політик | як `config` |
| `failed` | kopia server повернув помилку, яку повторна спроба не виправить, як-от неправильні облікові дані чи репозиторій лише для читання | як `config` |
| `state` | `STATE_FILE` відхилив зміну | як `config` |

Подробиці кожної проблеми є в логу, на рівні попередження або помилки.

### <a id="kopia_watcher_managed_sources-and-kopia_watcher_source_paused"></a>`kopia_watcher_managed_sources` і `kopia_watcher_source_paused`

`kopia_watcher_managed_sources` підраховує керовані джерела за `state`: `active` або `paused`, поки жоден із їхніх
контейнерів не працює. Обидва ряди є завжди. `kopia_watcher_source_paused` дорівнює 1 для кожного джерела на
паузі й 0 для кожного активного: джерело, чиї контейнери зупинено навмисно, не отримує запланованих знімків,
тож алерти про застарілі знімки його оминають.

### `kopia_watcher_source_owner`

1 для кожного контейнера, у мітках якого вказано шлях джерела, зокрема зупиненого. `container` — назва
контейнера, яка не змінюється, коли docker compose створює контейнер заново. Джерело, усіх контейнерів якого вже
немає, тут ряду не має; воно лишається керованим і на паузі, доки його не видалить очищення, якщо воно
налаштоване.

Використовуйте її в join, щоб показати контейнери джерела, наприклад останній успіх з одним рядом на
контейнер:

```promql
kopia_watcher_source_owner * on(path) group_left kopia_watcher_last_success_timestamp_seconds
```

### `kopia_watcher_hook_failures_total`

Команди [хуків](labels.md#hooks) контейнерів джерела, що завершилися помилкою, за `hook`: `pre-exec`, після якого
знімок не робиться, або `post-exec`. Хук завершується помилкою, коли виходить із кодом, відмінним від 0, не
закінчується за `kopia.hook-timeout`, не може запуститися (контейнер призупинено, команди не існує) або його
мітки мають помилку. У логу є контейнер і кінець виведення команди.

Ряди джерела з хуками починаються з 0, для обох хуків, коли джерело стає керованим, і зникають, коли жоден
контейнер джерела більше не має хуків. Хук, що завершився помилкою до того, як перший прохід по новому джерелу
звітував про це джерело, змушує свій ряд з'явитися зі значенням 1.

### `kopia_watcher_hook_duration_seconds`

Скільки тривали команди хуків, за `hook`, зокрема ті, що завершилися помилкою; хук, який не запустився, бо
Docker відмовив у виконанні exec або його мітки мають помилку, не враховується. Кошики гістограми: 0,1 с, 0,5 с, 1 с, 5 с,
15 с, 30 с, 1 хв, 5 хв, 15 хв, 30 хв, 1 год і 2 год. Обидва ряди є завжди.

### `kopia_watcher_backup_downtime_seconds`

Скільки тривав простій контейнерів, які watcher [зупинив чи призупинив](labels.md#stopping-and-pausing) для
знімка, за `mode`: `stop` або `pause`, від моменту безпосередньо перед тим, як watcher зупинив чи призупинив
контейнер, до моменту, коли він знову запустив чи відновив його. Контейнер, що повернувся без участі watcher,
бо його запустив хтось інший, не враховується. Кошики гістограми: 1 с, 5 с, 15 с, 30 с, 1 хв, 2 хв, 5 хв,
10 хв, 30 хв, 1 год, 2 год і 6 год. Обидва ряди є завжди.

### `kopia_watcher_return_failures_total`

Невдалі спроби знову запустити чи відновити контейнер, який watcher зупинив чи призупинив для знімка джерела за
шляхом `path`, за контейнером; watcher продовжує спроби з дедалі більшою затримкою. Ряд з'являється з першим
збоєм і зникає разом із джерелом. Зростання означає, що не працює контейнер, який мав би працювати:

```yaml
      - alert: KopiaContainerLeftDown
        expr: increase(kopia_watcher_return_failures_total[15m]) > 0
        annotations:
          summary: "{{ $labels.container }} did not come back after the snapshot of {{ $labels.path }}"
```

### `kopia_watcher_notifications_total`

Сповіщення на [`NOTIFY_WEBHOOK_URL`](configuration.md#notifications), за тим, що з ними сталося. Кожен
`result` починається з 0, навіть без вебхука.

| `result` | Коли враховується |
|---|---|
| `sent` | вебхук відповів кодом 2xx |
| `failed` | усі спроби завершилися помилкою або вебхук відповів так, що повторна спроба не допоможе, як-от 400 чи 404; причину вказано в логу |
| `dropped` | уже чекали 64 сповіщення або watcher зупинився раніше, ніж сповіщення було надіслано |

## <a id="scraping"></a>Збирання метрик

Приклад у [examples/](../../examples/) не публікує порт watcher. Prometheus у тій самій мережі Docker збирає
метрики за назвою сервісу:

```yaml
scrape_configs:
  - job_name: kopia-docker-watcher
    static_configs:
      - targets: ["watcher:8080"]
```

Метрики змінюються під час проходів звіряння, тож типового інтервалу збирання в одну хвилину цілком достатньо.

## <a id="alerts"></a>Алерти

Приклади, які варто адаптувати; пороги розраховано на щоденні знімки.

```yaml
groups:
  - name: kopia-docker-watcher
    rules:
      # No complete snapshot for over a day, except of paused sources.
      - alert: KopiaBackupStale
        expr: |
          (time() - kopia_watcher_last_success_timestamp_seconds > 26 * 3600)
            unless on(path) (kopia_watcher_source_paused == 1)
        for: 15m
        annotations:
          summary: "No complete snapshot of {{ $labels.path }} for over 26 hours"

      # An active source that has never had a complete snapshot.
      - alert: KopiaBackupMissing
        expr: |
          (kopia_watcher_source_paused == 0)
            unless on(path) kopia_watcher_last_success_timestamp_seconds
        for: 26h
        annotations:
          summary: "No complete snapshot of {{ $labels.path }} yet"

      # The second part catches a failure before the first scrape of a new
      # source, which increase() cannot see.
      - alert: KopiaWatcherSnapshotFailed
        expr: |
          increase(kopia_watcher_snapshot_failures_total[1h]) > 0
            or (kopia_watcher_snapshot_failures_total > 0 unless kopia_watcher_snapshot_failures_total offset 1h)
        annotations:
          summary: "A snapshot of {{ $labels.path }} the watcher took did not complete"

      # A hook failed: after a failed pre-exec no snapshot was taken.
      - alert: KopiaWatcherHookFailed
        expr: |
          increase(kopia_watcher_hook_failures_total[1h]) > 0
            or (kopia_watcher_hook_failures_total > 0 unless kopia_watcher_hook_failures_total offset 1h)
        annotations:
          summary: "The {{ $labels.hook }} hook of {{ $labels.path }} failed; see the watcher's log"

      # Fires when a problem comes up. Most are counted again at every full
      # reconcile while they last, label errors only while they hold back
      # the source of a running container. Otherwise, as for a container
      # whose path cannot be used, they count once, and the alert resolves
      # while the problem stays.
      - alert: KopiaWatcherErrors
        expr: increase(kopia_watcher_reconcile_errors_total{kind!~"temporary|unavailable|docker"}[30m]) > 0
        annotations:
          summary: "The watcher cannot apply the labels ({{ $labels.kind }}); see its log"

      # The notifications do not reach the webhook.
      - alert: KopiaWatcherNotificationsFailing
        expr: increase(kopia_watcher_notifications_total{result!="sent"}[1h]) > 0
        annotations:
          summary: "Notifications to NOTIFY_WEBHOOK_URL were {{ $labels.result }}; see the watcher's log"

      - alert: KopiaWatcherDown
        expr: up{job="kopia-docker-watcher"} == 0
        for: 5m
```
