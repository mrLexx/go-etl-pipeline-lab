# Go ETL pipeline Lab

[![Go Version](https://img.shields.io/badge/Go-1.26-blue.svg)](https://go.dev/)
![Go Concurrency ](https://img.shields.io/badge/Go-Concurrency-blue.svg)
![CI](https://github.com/mrLexx/go-etl-pipeline-lab/actions/workflows/go.yml/badge.svg)
![Coverage](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/mrLexx/d259eff64b03c387a366fbd4589b0719/raw/67d6aa50f9bbb30277122eda7d509b30a2db72b1/go-etl-pipeline-lab.json)

Учебный проект по реализации ETL Pipeline на Go.

## Запуск

В проекте используется инструмент [Task](https://taskfile.dev) для автоматизации сборки, тестирования и запуска. Он заменяет классический `Makefile`.

### 1. Установка утилиты Task

Перед началом работы установите `task` на ваш компьютер.

> Полный список способов установки доступен в [официальной документации](https://taskfile.dev).

### 2. Доступные команды

Чтобы увидеть весь список доступных команд с описанием, выполните в корне проекта:

```bash
task --list
```

**Основные команды для разработки:**

<!-- TASKS_START -->
* `task coverage:check` — Проверяет покрытие без запуска тестов
* `task coverage:html` — Генерирует HTML-отчёт покрытия
* `task deps:update` — Обновляет зависимости
* `task docs` — Обновить список команд в README.md
* `task fix:apply` — Производит предварительный просмотр автоматических исправлений
* `task fix:diff` — Производит предварительный просмотр автоматических исправлений
* `task format` — Форматирует код (gofumpt + gci)
* `task formatters:install` — Устанавливает gofumpt и gci
* `task golangci-lint:install` — Устанавливает golangci-lint
* `task install` — Устанавливает все инструменты
* `task lint` — Запускает golangci-lint
* `task run` — Запускает проект
* `task test` — Запускает unit-тесты с race-детектором
* `task test:coverage` — Тесты с покрытием
<!-- TASKS_END -->

### 3. Порядок запуска

Обновляем зависимости:

```bash
task deps:update
```

Запускаем сам проект

```bash
task run
```
