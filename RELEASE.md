# Инструкция по созданию релиза v0.0.1

## Шаги для создания релиза:

### 1. Убедитесь, что все изменения закоммичены и запушены в ваш форк

```bash
cd c:\work\devops\chart\terraform-clickhouse
git add .
git commit -m "Prepare release v0.0.1"
git push origin master
```

### 2. Создайте тег версии

```bash
git tag -a v0.0.1 -m "Release v0.0.1"
git push origin v0.0.1
```

### 3. Соберите провайдер для разных платформ

Убедитесь, что Go установлен (https://golang.org/dl/), затем:

```bash
# Для Windows (amd64)
go build -o terraform-provider-clickhouse_v0.0.1_windows_amd64.exe

# Для Linux (amd64)
GOOS=linux GOARCH=amd64 go build -o terraform-provider-clickhouse_v0.0.1_linux_amd64

# Для macOS (amd64)
GOOS=darwin GOARCH=amd64 go build -o terraform-provider-clickhouse_v0.0.1_darwin_amd64
```

### 4. Создайте GitHub Release

1. Перейдите на https://github.com/Fox052-byte/terraform-clickhouse/releases/new
2. Выберите тег `v0.0.1`
3. Заголовок: `v0.0.1`
4. Описание: `Initial release of forked ClickHouse Terraform provider`
5. Загрузите собранные бинарники:
   - `terraform-provider-clickhouse_v0.0.1_windows_amd64.exe`
   - `terraform-provider-clickhouse_v0.0.1_linux_amd64`
   - `terraform-provider-clickhouse_v0.0.1_darwin_amd64`
6. Нажмите "Publish release"

### 5. Использование в Terraform

После создания релиза, обновите `versions.tf`:

```hcl
terraform {
  required_providers {
    clickhouse = {
      source  = "Fox052-byte/clickhouse"
      version = "0.0.1"
    }
  }
}
```

## Альтернатива: Использование GitHub Actions

Можно настроить GitHub Actions для автоматической сборки и создания релизов при создании тега.

