#!groovy

// Copyright 2017 Bitwise IO, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// ------------------------------------------------------------------------------

pipeline {
    agent {
        node {
            label 'master'
            customWorkspace "workspace/${env.BUILD_TAG}"
        }
    }

    triggers {
        cron(env.BRANCH_NAME == 'master' ? 'H 2 * * *' : '')
    }

    options {
        timestamps()
        buildDiscarder(logRotator(daysToKeepStr: '31'))
    }

    environment {
        ISOLATION_ID = sh(returnStdout: true, script: 'printf $BUILD_TAG | sha256sum | cut -c1-64').trim()
        COMPOSE_PROJECT_NAME = sh(returnStdout: true, script: "printf $BUILD_TAG | sha256sum | cut -c1-64").trim()
    }

    stages {
        stage('Check Whitelist') {
            steps {
                readTrusted 'bin/whitelist'
                sh './bin/whitelist "$CHANGE_AUTHOR" /etc/jenkins-authorized-builders'
            }
            when {
                not {
                    branch 'master'
                }
            }
        }

        stage("Run lint") {
            steps {
                sh './bin/run_lint'
            }
        }

        stage("Run unit tests") {
            steps {
                sh './bin/run_tests'
            }
        }

        stage("Build rust docs") {
            steps {
                sh 'docker-compose -f docker/compose/docker-compose.yaml run --rm transact bash -c "cd /project/transact && cargo doc"'
            }
        }
    }
    post {
        always {
            sh 'docker-compose -f docker/compose/docker-compose.yaml down'
        }
        success {
            archiveArtifacts 'target/doc/**/*.html, target/doc/**/*.woff, target/doc/**/*.txt, target/doc/**/*.css, target/doc/**/*.js'
        }
    }
}
