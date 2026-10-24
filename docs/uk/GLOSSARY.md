# Глосарій перекладу

Терміни, якими перекладено документацію kopia-docker-watcher українською. Англійський текст — першоджерело;
змінюючи його, оновіть відповідний файл у `docs/uk/` або `README.uk.md` з цими термінами або напишіть у pull
request, що переклад відстає. `make docs-check` показує відсутні й застарілі переклади та биті посилання.

## Що не перекладається

- Блоки коду й команд, зокрема коментарі в них, і все, що в тексті стоїть у `code`.
- Імена міток (`kopia.path`), змінних середовища (`STATE_FILE`), полів JSON і політик Kopia
  (`scheduling.manual`), метрик, прапорців і підкоманд (`--dry-run`, `validate`), URL і шляхи до файлів.
- Назви: kopia server, Kopia, Docker, compose, docker compose, Prometheus, Terraform, terraform-provider-kopia,
  ntfy, Slack, Telegram, kopia-docker-operator, kopia-docker-watcher.
- watcher — назва програми, пишемо латиницею без перекладу й не відмінюємо: «watcher реєструє джерело»,
  «розклад watcher». На початку речення — з великої літери: «Watcher реєструє джерело».
- Тексти повідомлень у логу, які цитує документація («container labels have errors»), лишаються англійською.
- Якорі посилань: українські сторінки мають ті самі якорі, що й англійські (`labels.md#kopiavolumes`,
  `labels.md#hooks`), щоб посилання вели в те саме місце обома мовами.

## Терміни

| English | Українською | Примітка |
|---|---|---|
| alert | алерт, правило алерту | Prometheus |
| backed-up directory | каталог, що потрапляє в резервну копію | |
| backup | резервна копія, резервне копіювання | |
| bind mount | bind-монтування | |
| boolean | булеве значення | |
| checkpoint | контрольна точка | вивантаження знімка |
| cleanup | очищення | `kopia.cleanup-on-remove` |
| complete, incomplete snapshot | повний, неповний знімок | |
| container | контейнер | |
| control socket | керувальний сокет | `CONTROL_SOCKET` |
| crash, crash-looping container | збій, падіння; контейнер, що циклічно падає | |
| credentials | облікові дані | |
| cron expression | вираз cron | |
| default | типове значення; типово | не «за замовчуванням» |
| depend on | залежати від | `kopia.depends-on` |
| deploy | розгортання; розгорнути | |
| downtime | простій | |
| dry run | пробний запуск | `run --dry-run` |
| dump | дамп | дамп бази даних |
| duration | тривалість | Go duration — тривалість у форматі Go |
| effective policy | чинна політика | |
| endpoint | ендпоінт | |
| environment variable | змінна середовища | |
| error | помилка | |
| event | подія | Docker |
| exit code, exit status | код виходу | |
| fingerprint | відбиток | сертифіката |
| full reconcile | повне звіряння | |
| global policy, host policy | глобальна політика, політика хоста | |
| grace period | відстрочка | `CLEANUP_GRACE` |
| health check | перевірка стану | `/healthz`, `HEALTHCHECK` |
| hook | хук | `kopia.pre-exec`, `kopia.post-exec` |
| host path | шлях на хості | монтування |
| ignore rules | правила ігнорування | |
| image | образ | Docker |
| inherit | успадковувати | поле політики |
| interval | інтервал | |
| label | мітка | Docker label; label prefix — префікс міток |
| log; is logged | лог, у логу; записується в лог | не «у лозі», не «пише в лог» |
| managed source | кероване джерело | |
| mount | монтування | |
| mount destination | точка монтування | шлях у контейнері |
| notification, notify | сповіщення; надіслати сповіщення | |
| output | виведення | команди чи хука; не «вивід» |
| owner, own | власник; володіти, належати | хто відповідає за джерело чи налаштування |
| parent policy | батьківська політика | |
| pass (reconcile pass) | прохід (прохід звіряння) | |
| path | шлях | |
| pause (of a container) | призупинення; призупинити, відновити | `docker pause`, `kopia.pause-during-backup` |
| pause (of a source) | пауза; ставити на паузу, знімати з паузи | `scheduling.manual=true` |
| pin, pinned snapshot | закріплення; закріплений знімок | `kopia.pin-on-stop` |
| placeholder | заповнювач | `{date}`, `{time}` |
| policy | політика | політика Kopia |
| policy field | поле політики | |
| policy labels | мітки політики | |
| queue | черга; поставити в чергу | |
| reconcile | звіряння; звіряти | узгодження джерела з мітками |
| reconcile loop | цикл звіряння | |
| release, pre-release | реліз, передреліз | |
| repository | репозиторій | Kopia |
| restart policy | політика перезапуску | Docker |
| restore | відновлення; відновити | команда `restore` |
| retention | зберігання | правила зберігання знімків |
| retry | повторна спроба; повторювати спроби | |
| schedule | розклад | |
| scheduled snapshot | запланований знімок | |
| scrape, scrape configuration | збирання метрик, конфігурація збирання | Prometheus |
| self-signed certificate | самопідписаний сертифікат | |
| series, histogram bucket | ряд, кошик гістограми | Prometheus |
| shell command | команда оболонки | |
| snapshot | знімок | |
| snapshot at a start / at a stop | знімок під час запуску / під час зупинки | контейнера |
| source | джерело | джерело знімків у kopia server |
| state file | файл стану | `STATE_FILE` |
| stop (of a container) | зупинка; зупинити, запустити знову | `kopia.stop-during-backup` |
| strict mode | суворий режим | `--strict` |
| Summary (section) | Зведення | заголовок розділу з таблицею |
| task | завдання | kopia server |
| throttle | обмеження швидкості | `KOPIA_THROTTLE_*` |
| time of day, time zone | час доби, часовий пояс | |
| timeout | тайм-аут | |
| trailing slash | кінцева скісна риска | |
| typo | одрук | |
| upload, download | вивантаження, завантаження | |
| usage error | помилка використання | код виходу 2 |
| volume | том | Docker volume |
| warning | попередження | |
| web UI | вебінтерфейс | Kopia web UI — вебінтерфейс Kopia |
| webhook | вебхук | |

## Стиль

- Звичайна технічна українська, речення короткі, як в оригіналі. Звертання до читача — на «ви», без
  великої літери.
- Без кальок із російської: «є», а не «являється»; «наступний», а не «слідуючий»; «протягом», а не «на
  протязі»; «збігатися», а не «співпадати»; «у разі», а не «у випадку»; «виняток», а не «виключення»;
  «будь-який», а не «любий»; «правильний», а не «вірний»; «вважати», а не «рахувати».
- Лапки — «ялинки»; лапки всередині лапок — „лапки“.

## Як оновлювати переклад

Кожен перекладений файл починається з коментаря, що називає англійський файл і коміт, з якого зроблено переклад:

```
<!-- translated-from: docs/labels.md @ <commit> -->
```

Другий рядок — перемикач мови. `make docs-check` порівнює англійський файл у робочому дереві з його версією в
цьому коміті й попереджає, якщо файл відтоді змінився; показує й команду `git diff`, щоб побачити зміни.
Перекладіть їх і вкажіть у коментарі коміт, у якому англійський файл уже має поточний текст, наприклад коміт
англійської зміни. Відсутній переклад, переклад без англійського файлу, неправильний коментар або коміт, якого
немає в історії, — помилка.
