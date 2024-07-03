## Counter

This repository contains the implementation of a "named counter" type for use with the `gorm` ORM.

### Running Tests

Executing the test suite requires PostgreSQL container running on the same system as test execution. Test setup assumes that the PostgreSQL instance is available on `localhost` at port `5432` under the username `postgres`.

Start a PostgreSQL container for use in test suite:

```bash
docker run --rm --name postgres -p 5432:5432 -e POSTGRES_PASSWORD=aide postgres
```
