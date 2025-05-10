# Trying out graph database

Graph databases is an interesting topic. Graph theory was a beloved module at university. Unfortunately I was not able to use them in professional life.
After discovering that SQL server has graph extensions I was willing to try them. It just seemed like something not completely unknown and still holding potential of learning something new.

Time has come. Let's check it out: look into query complexity and performance. For performance a lot of data is needed. [IMDB](https://www.imdb.com/interfaces/)'s database is a perfect candidate for that.

## Schemas
| Graph | Relational |
| ----- | ---------- |
| ![graph_schemawsd](https://www.plantuml.com/plantuml/svg/ROyn2iCm34LtdS8No0Kwb6oT2jrBCMhzunWI6MIvqDitfNQcRF_1rvCq5cErkYv45uZYv8HN45tpEtLe-GFMdXT8j7adbbWvzZ4trE7icYuLdoUY6xHaQS8E4HAWEpwJskm36th_RsfRVfSgWa_YxbmUHvMG52z381etzVRL5m00 "graph_schemawsd") | |


## Dev notes
### Run SQL Server
```powershell
docker run -e "ACCEPT_EULA=Y" -e "MSSQL_SA_PASSWORD=test-1234" -p 1433:1433 --user root -v ./mssql_data:/var/opt/mssql/data -d mcr.microsoft.com/mssql/server:latest
```

### Cleanup data
```sql
truncate table is_of;
truncate table genre;
truncate table movie;
```
