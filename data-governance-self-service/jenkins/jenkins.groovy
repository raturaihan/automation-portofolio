pipeline {
    agent any

    options {
        withFolderProperties()
    }

    parameters {
        string(name: 'PERSONAL_TOKEN', description: 'Personal Token (Data Service)')
        string(name: 'SPREADSHEET_ID', description: 'Spreadsheet ID (add xxx as an editor)')
        
    }

    environment {
        APP_KEY = credentials('APP_KEY')
        APP_SECRET = credentials('APP_SECRET')

    }

    stages {
        stage('env_check') {
            steps {
                echo "Running validation check..."
                sh """
                  ${env.PYTHON_PATH} update_data_owner.py \
                    --mode env_check \
                    --sheet-id ${params.SPREADSHEET_ID} \
                """
            }
        }
        stage('check') {
            steps {
                echo "Running validation check..."
                sh """
                  ${env.PYTHON_PATH} update_data_owner.py \
                    --mode check \
                    --sheet-id ${params.SPREADSHEET_ID} \
                """
            }
        }
        stage('update') {
            steps {
                echo "Updating DataHub owners/editors..."
                withEnv([
                    "PERSONAL_TOKEN=${params.PERSONAL_TOKEN}"
                ]) {
                    sh """
                    ${env.PYTHON_PATH} update_data_owner.py \
                        --mode update \
                        --sheet-id ${params.SPREADSHEET_ID} \
                        --app-key $APP_KEY \
                        --app-secret $APP_SECRET
                    """
                }
            }
        }

    }

    post { 
        cleanup { 
            cleanWs()
        }
    }
}
