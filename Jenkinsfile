#!groovy

@Library("boxed-kube-infrastructure") _

node('docker-builder') {
    setupBuild()
    stage("updating external-data-importer jobs") {
        sh("freighter build-and-push deploy/config/external-data-importer.config.yml")
    }
}
