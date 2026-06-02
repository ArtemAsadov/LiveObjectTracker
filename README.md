# Live Object Tracker

Пет-проект для отработки паттернов high-load на .NET 8.

## Задачи и шаги

### 1. Прием TCP-потока через `BoundedChannel` с backpressure
- **1.1** Модель `CoordinateEvent` + скелет Program.cs
- **1.2** TCP-сервер: принимает события → пишет в `BoundedChannel` (10k, Wait)
- **1.3** Воркеры на `TaskCreationOptions.LongRunning`, читают из канала
- **1.4** Генератор-клиент: шлет ~10k RPS
- **1.5** Метрики в консоль (produced/consumed RPS, pending, cache size)

### 2. Кэш позиций на `ReaderWriterLockSlim` + бенчмарк
- **2.1** Самописный `PositionsCache` (`Dictionary` + `ReaderWriterLockSlim`)
- **2.2** Замена `ConcurrentDictionary` из задачи 1 на новый кэш
- **2.3** Бенчмарк: RWLock vs `lock` (read-heavy нагрузка)
- **2.4** Глобальный счетчик через `Interlocked.Increment`

### 3. Батчевая запись в Postgres (COPY) и ClickHouse (MergeTree)
- **3.1** SQL-скрипты для таблиц в PG и CH
- **3.2** Batch-канал: воркеры сбрасывают события отдельным читеру
- **3.3** Writer в Postgres через `NpgsqlBinaryImporter`
- **3.4** Writer в ClickHouse через `ClickHouse.Client`
- **3.5** Сравнительный аналитический запрос (PG vs CH)

### 4. WPF 3D клиент на HelixToolkit с Grid-разбиением
- **4.1** WPF проект + HelixToolkit, пустая 3D сцена с камерой
- **4.2** Рендер 5000 статических сфер
- **4.3** Структура Grid (сетка 100x100), распределение сфер по ячейкам
- **4.4** Frustum culling: рендер только ячеек, пересекающихся с камерой
- **4.5** Подключение к серверу из задачи 1 (координаты в реальном времени)
- **4.6** Переключатель Grid on/off + замер FPS
=======
