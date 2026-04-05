# RxJava Custom Implementation

Собственная реализация RxJava - библиотеки для построения асинхронных программ с
использованием реактивного подхода.

---

## Обзор

Образовательная реализация, демонстрирующая внутреннюю архитектуру библиотек реактивного программирования. Основана на
ключевых концепциях RxJava:

- `Observable` - источники данных
- `Observer` - получатели событий
- Операторы - трансформация, фильтрация, управление потоками
- `Scheduler` - управление потоками исполнения
- Обработка ошибок - глобальный обработчик необрабатываемых ошибок

---

## Пример пайплайна

```java
import com.dfedorino.rxjava.core.Disposable;
import com.dfedorino.rxjava.core.Observable;
import com.dfedorino.rxjava.core.Observer;
import com.dfedorino.rxjava.scheduler.Schedulers;

public class Example {
    public static void main(String[] args) {
        Observable.<String>create(emitter -> {
                    System.out.printf("[%s] Emitting%n", Thread.currentThread().getName());
                    emitter.onNext("Hello");
                    emitter.onNext("World");
                    emitter.onComplete();
                })
                .map(String::toUpperCase)
                .filter(s -> s.length() > 4)
                .flatMap(s -> Observable.<String>create(e -> {
                    e.onNext(s + "!");
                    e.onComplete();
                }))
                .subscribeOn(Schedulers.io())
                .observeOn(Schedulers.single())
                .subscribe(new Observer<>() {
                    @Override
                    public void onSubscribe(Disposable d) {
                        System.out.printf("[%s] Subscribed%n", Thread.currentThread().getName());
                    }

                    @Override
                    public void onNext(String item) {
                        System.out.printf("[%s] Received: %s%n", Thread.currentThread().getName(), item);
                    }

                    @Override
                    public void onError(Throwable e) {
                        e.printStackTrace();
                    }

                    @Override
                    public void onComplete() {
                        System.out.printf("[%s] Done%n", Thread.currentThread().getName());
                    }
                });
    }
}
```

### Путь элементов через пайплайн

```
[main] ──────────────────────────────────────────────────────────────────────

                          onSubscribe(Disposable)
                                   │
                                   ▼
                          ┌────────────────┐
                          │   subscribe    │
                          └───────┬────────┘
                                  │

[rxjava-io-1] ── subscribeOn schedules subscription on IO thread ──────────

                                  │
                                  ▼
                          ┌────────────────────┐
                          │     create         │
                          │  "Emitting" print  │
                          │  onNext("Hello")   │
                          │  onNext("World")   │
                          │  onComplete()      │
                          └────────┬───────────┘
                                   │
                                   ▼
                          ┌────────────────────┐
                          │  map(toUpperCase)  │
                          │  "Hello"→"HELLO"   │
                          │  "World"→"WORLD"   │
                          └────────┬───────────┘
                                   │
                                   ▼
                          ┌────────────────────┐
                          │  filter(len > 4)   │
                          │  "HELLO" (6)       │
                          │  "WORLD" (5)       │
                          └────────┬───────────┘
                                   │
                                   ▼
                          ┌────────────────────┐
                          │    flatMap         │
                          │  "HELLO"→"HELLO!"  │
                          │  "WORLD"→"WORLD!"  │
                          └────────┬───────────┘
                                   │
                                   ▼
                          ┌────────────────────┐
                          │ observeOn(single)  │
                          │  (queues + drains  │
                          │   to single thread)│
                          └────────┬───────────┘
                                   │

[rxjava-single] ── observeOn schedules downstream on single thread ─────────

                                   │
                                   ▼
                          ┌────────────────────┐
                          │   subscribe        │
                          │ onNext("HELLO!")   │
                          │ onNext("WORLD!")   │
                          │ onComplete()       │
                          │  "Done" print      │
                          └────────────────────┘
```

Результат выполнения:

```
[main] Subscribed
[rxjava-io-1] Emitting
[rxjava-single] Received: HELLO!
[rxjava-single] Received: WORLD!
[rxjava-single] Done
```

---

### Шедулеры

| Scheduler                  | Описание                                 | Когда использовать                         |
|----------------------------|------------------------------------------|--------------------------------------------|
| `Schedulers.io()`          | Кэшированный пул потоков                 | Сетевые запросы, файловый I/O, базы данных |
| `Schedulers.computation()` | Фиксированный пул потоков (по числу CPU) | CPU-ёмкие вычисления, трансформации данных |
| `Schedulers.single()`      | Один фоновый поток                       | Последовательное выполнение, UI-обновления |

---

## Архитектура

### Обзор

```
rxjava/
├── core/           # Базовые компоненты (Observable, Observer, Disposable)
├── operators/      # Операторы (transform, predicate, threading)
│   ├── transform/  # map, flatMap
│   ├── predicate/  # filter
│   └── threading/  # subscribeOn, observeOn
├── scheduler/      # Планировщики (IO, Computation, Single)
└── exception/      # Глобальная обработка ошибок (ErrorHandlers)
```

---

### Пакет `core` — Базовые компоненты

Пакет `com.dfedorino.rxjava.core` содержит основные интерфейсы и классы, реализующие паттерн Observer и
обеспечивающие работу реактивных потоков.

| Класс/Интерфейс            | Описание                                                                                                                                                                                                                                                              |
|----------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `Observer<T>`              | Интерфейс получателя событий от Observable. Определяет 4 метода жизненного цикла: `onSubscribe` (подписка), `onNext` (элемент), `onError` (ошибка), `onComplete` (завершение)                                                                                         |
| `Observable<T>`            | Абстрактный класс — источник данных. Содержит абстрактный метод `subscribeActual` для реализации подписки и статический `create` для создания Observable с кастомной логикой. Предоставляет методы операторов: `map`, `filter`, `flatMap`, `subscribeOn`, `observeOn` |
| `ObservableEmitter<T>`     | Интерфейс для эмиссии элементов наблюдателю. Расширяет `Disposable`, позволяя источнику данных управлять состоянием подписки. Определяет `onNext`, `onError`, `onComplete`                                                                                            |
| `ObservableOnSubscribe<T>` | Функциональный интерфейс с методом `subscribe(ObservableEmitter<T>)`. Используется в `Observable.create()` для определения пользовательской логики генерации данных                                                                                                   |
| `CreateEmitter<T>`         | Внутренняя реализация `ObservableEmitter`. Обеспечивает управление состоянием через disposed/terminated. Гарантирует однократный вызов терминальных событий и блокировку эмиссии после завершения                                                                     |
| `Disposable`               | Интерфейс для отписки и освобождения ресурсов. Методы: `dispose()` (отмена подписки), `isDisposed()` (проверка состояния).                                                                                                                                            |

---

### Пакет `operators` — Операторы трансформации, фильтрации и управления потоками

Пакет содержит реализации операторов, позволяющих модифицировать, фильтровать и управлять потоком данных. Каждый
оператор реализован по паттерну XxxObservable + XxxObserver.

### Реализация Operator

Каждый оператор состоит из двух классов:

- XxxObservable - обёртка над источником, хранит конфигурацию
- XxxObserver - обёртка над наблюдателем, перехватывает/модифицирует события

```
Source Observable
       │
       ▼
  XxxObservable  ← хранит source + оператор
       │
       ▼
  XxxObserver    ← перехватывает onNext, применяет логику
       │
       ▼
Downstream Observer
```

#### Подпакет `transform` — Операторы трансформации (`map`, `flatMap`)

| Класс/Интерфейс           | Описание                                                                                                                                                                                                                                                                |
|---------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `MapObservable<T, R>`     | Хранит источник и функцию-маппер. В `subscribeActual` подписывает `MapObserver` на источник                                                                                                                                                                             |
| `MapObserver<T, R>`       | Перехватывает `onNext`, применяет mapper функцию и передаёт результат в downstream. Обрабатывает ошибки маппинга через `onError`                                                                                                                                        |
| `FlatMapObservable<T, R>` | Хранит источник и функцию, возвращающую внутренний Observable для каждого элемента                                                                                                                                                                                      |
| `FlatMapObserver<T, R>`   | Координирует множественные подписки на внутренние Observable. Использует `AtomicInteger` для подсчёта активных подписок, `ConcurrentLinkedQueue` для хранения Disposable внутренних подписок. Завершается когда все внутренние Observable завершены и источник завершён |
| `FlatMapInnerObserver<R>` | Подписывается на внутренний Observable, передаёт результаты в `FlatMapObserver`. Отслеживает disposed-состояние родителя перед эмиссией                                                                                                                                 |

#### Подпакет `predicate` — Операторы фильтрации (`filter`)

| Класс/Интерфейс       | Описание                                                                                                                                                                             |
|-----------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `FilterObservable<T>` | Хранит источник и Predicate. В `subscribeActual` подписывает `FilterObserver` на источник                                                                                            |
| `FilterObserver<T>`   | Перехватывает `onNext`, вызывает Predicate.test(item). Пропускает элемент в downstream только если предикат возвращает `true`. Ошибки, возникшие в предикате, передаются в `onError` |

#### Подпакет `threading` — Операторы управления потоками (`subscribeOn`, `observeOn`)

| Класс/Интерфейс            | Описание                                                                                                                                                                                                                                                                                                                        |
|----------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `SubscribeOnObservable<T>` | Хранит источник и Scheduler. Выполняет подписку на источник в фоновом потоке Scheduler. Передаёт Disposable downstream до планирования задачи                                                                                                                                                                                   |
| `SubscribeOnObserver<T>`   | Сохраняет upstream Disposable через `AtomicReference` для корректной отписки. Проверяет disposed-состояние перед передачей событий                                                                                                                                                                                              |
| `ObserveOnObservable<T>`   | Хранит источник и Scheduler. Подписывается на источник напрямую, но переключает обработку событий downstream на Scheduler через очередь и drain-цикл                                                                                                                                                                            |
| `ObserveOnObserver<T>`     | Использует `ConcurrentLinkedQueue` для буферизации элементов, `AtomicInteger wip` для координации drain-задач. Метод `schedule()` запускает drain в Scheduler только если другой drain ещё не выполняется. Drain-цикл последовательно извлекает элементы из очереди и доставляет их downstream, проверяя терминальное состояние |

Паттерн ObserveOn (drain-цикл):

```
Source Observable
       │ onNext/onError/onComplete
       ▼
ObserveOnObserver
       ├── queue.offer(item)        ← буферизация событий
       ├── wip.getAndIncrement()    ← координация drain-задач
       └── scheduler.execute(drain) ← запуск только если wip==0
              │
              ▼
         drain() цикл
              ├── queue.poll() → downstream.onNext(item)
              ├── checkTerminated() → downstream.onError/onComplete
              └── завершение когда очередь пуста + done=true
```

Разница `subscribeOn` vs `observeOn`:

| Оператор      | Что переключает                         | Когда влияет                        |
|---------------|-----------------------------------------|-------------------------------------|
| `subscribeOn` | Поток подписки на источник              | Только при подписке (однократно)    |
| `observeOn`   | Поток обработки всех downstream событий | На каждый onNext/onError/onComplete |

---

### Пакет `scheduler` — Планировщики выполнения задач

Пакет содержит абстракцию для управления потоками исполнения и готовые реализации для типовых сценариев (I/O, CPU,
последовательное выполнение).

| Класс/Интерфейс         | Описание                                                                                                                                                                             |
|-------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `Scheduler`             | Интерфейс с методами: `execute(Runnable)` — исполнение задачи, `shutdown()` — корректное завершение (graceful + force через 5s), `isShutdown()` — проверка состояния                 |
| `IOThreadScheduler`     | Реализация на базе `CachedThreadPool`. Динамически создаёт потоки под нагрузку, переиспользует свободные. Потоки с именами `rxjava-io-N`. Для I/O-операций (сеть, файлы, БД)         |
| `ComputationScheduler`  | Реализация на базе `FixedThreadPool` размером `Runtime.availableProcessors()`. Потоки-демоны с именами `rxjava-computation-N`. Для CPU-ёмких вычислений (трансформации, агрегации)   |
| `SingleThreadScheduler` | Реализация на базе `SingleThreadExecutor`. Один поток `rxjava-single` для последовательного выполнения задач.                                                                        |
| `Schedulers`            | Фабрика синглтон-экземпляров шедулеров через holder-паттерн (`IoHolder`, `ComputationHolder`, `SingleHolder`) для ленивой инициализации. Методы: `io()`, `computation()`, `single()` |

---

### Обработка ошибок

`ErrorHandlers` - статический класс с глобальным обработчиком:

1. Если обработчик установлен - вызывается
2. Если ошибка в обработчике - передаётся uncaught handler потока
3. Если обработчика нет - напрямую в uncaught handler

Используется для ошибок, которые не могут быть доставлены подписчику (после dispose/onComplete).

---

### Тестирование

Пакет `src/test/java/com/dfedorino/rxjava/core` покрывает следующие ключевые сценарии:

| Тест                                    | Основные сценарии                                                                                                                                |
|-----------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------|
| CreateEmitterTest                       | Потокобезопасность dispose; остановка эмиссии после dispose; однократность терминальных событий; корректность isDisposed/isTerminated            |
| ObservableTest                          | Эмиссия элементов/ошибок/завершения; прекращение после dispose/onError/onComplete; обработка NPE при null source; доставка только первой onError |
| ObserverTest                            | Получение элементов, ошибок, завершения; последовательность обратных вызовов (onSubscribe → onNext → onComplete)                                 |
| ErrorHandlersTest                       | Регистрация/замена/сброс обработчика; fallback в System.err; ошибки внутри обработчика; однократный вызов; null обработчик                       |
| UndeliverableErrorsRxJavaComparisonTest | Сравнение с RxJava: onError/onNext после onComplete → undeliverable error                                                                        |
| FlatMapUndeliverableErrorsTest          | flatMap: ошибки после dispose (onError, innerError) → глобальный обработчик                                                                      |

Пакет `src/test/java/com/dfedorino/rxjava/operators` покрывает операторы трансформации, фильтрации и управления
потоками:

| Тест                           | Основные сценарии                                                                                                                                                                                                                                                   |
|--------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| MapOperatorTest                | Трансформация элементов; обработка пустого потока; проброс ошибок; исключение в mapper; остановка после dispose; цепочка map; null из mapper;                                                                                                                       |
| FilterOperatorTest             | Фильтрация по предикату; always-true/false предикаты; пустой поток; null-значения; цепочка filter; проброс ошибок; исключение в предикате; остановка после dispose; однократность вызова предиката; NPE при null предикате; повторный onComplete/onError            |
| FlatMapOperatorTest            | Преобразование в Observable и объединение; пустой источник/внутренний Observable; проброс ошибок (источник, внутренний, mapper); dispose всех внутренних; цепочка с map/filter; onComplete после всех внутренних; NPE при null mapper; повторный onComplete/onError |
| FlatMapUndeliverableErrorsTest | flatMap: onError после dispose; innerError после dispose → глобальный обработчик                                                                                                                                                                                    |
| SubscribeOnOperatorTest        | Подписка в потоке Scheduler; onNext в том же потоке; проброс ошибок; dispose до начала эмиссии; ошибка Scheduler; многократный subscribeOn                                                                                                                          |
| ObserveOnOperatorTest          | Переключение onNext/onError/onComplete на Scheduler; сохранение порядка; dispose; многократный observeOn; цепочка с subscribeOn/map; ошибка Scheduler; пустой поток; сравнение с RxJava reference                                                                   |

Пакет `src/test/java/com/dfedorino/rxjava/scheduler` покрывает планировщики выполнения задач и фабрику Schedulers:

| Тест                      | Основные сценарии                                                                                                                                                                                           |
|---------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| ComputationSchedulerTest  | Выполнение задач в потоках вычислений; RejectedExecutionException после shutdown; ограничение кол-ва потоков до CPU cores; конкурентные задачи; корректная остановка; использование ровно CPU cores потоков |
| IOThreadSchedulerTest     | Выполнение задач в I/O потоках; RejectedExecutionException после shutdown; переиспользование потоков из кэша; конкурентные задачи; корректная остановка; создание новых потоков при перегрузке              |
| SingleThreadSchedulerTest | Выполнение в одиночном потоке; RejectedExecutionException после shutdown; последовательное выполнение; сохранение порядка; корректная остановка; все задачи на одном потоке                                 |
| SchedulerContractTest     | Параметризованные тесты для всех шедулеров: выполнение до shutdown; RejectedExecutionException после shutdown; корректное состояние isShutdown; многократный shutdown                                       |
| SchedulersFactoryTest     | Singleton-инстансы io()/computation()/single(); проверка на null; одинаковость экземпляров при повторных вызовах                                                                                            |
| RejectedExecutionTest     | RejectedExecutionException в subscribeOn → проброс в onError                                                                                                                                                |


