### 起動の前提
1. [aws-mwaa-local-runner](https://github.com/aws/aws-mwaa-local-runner)の利用する前提のdagです
2. 該当リポジトリのdags/に配置して利用
3. docker/config/.env.localrunnerに環境変数の設定
4. requirements/requiremets.txtに以下追記必要

```
apache-airflow-providers-databricks
```