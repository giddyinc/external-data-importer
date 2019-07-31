
#!groovy

@Library("boxed-kube-infrastructure") _

node {

    switch (env.BRANCH_NAME) {
        case "master":
            env.APPS = "external-data-importer"
            namespace = "prod"
            environment = "prod"
            dag_bucket = "boxed-production-dags"
            plugins_bucket = "boxed-production-airflow-plugins"
            break
        case "development":
            env.APPS = "external-data-importer"
            namespace = "staging"
            enviroment = "staging"
            dag_bucket = "boxed-staging-dags"
            plugins_bucket = "boxed-staging-airflow-plugins"
            break
    }
    setupFreighter()

    stage("updating external-data-importer jobs") {
        sh("freighter build-and-push deploy/config/external-data-importer.config.yml")
    }
}

node {
    cleanWs()
}