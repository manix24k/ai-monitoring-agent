pipeline {
  agent any

  options {
    timestamps()
    disableConcurrentBuilds()
    buildDiscarder(logRotator(numToKeepStr: '100'))
  }

  parameters {
    string(name: 'SERVICE_NAME', defaultValue: '', trim: true, description: 'Failing service name')
    choice(name: 'TARGET_ENV', choices: ['venus', 'jupiter'], description: 'Target namespace')
    string(name: 'TARGET_JOB', defaultValue: '', trim: true, description: 'Optional explicit Jenkins job full path')
    booleanParam(name: 'DRY_RUN', defaultValue: false, description: 'Resolve and print only')
  }

  stages {
    stage('Validate') {
      steps {
        script {
          if (!(params.TARGET_ENV in ['venus', 'jupiter'])) {
            error("TARGET_ENV must be venus or jupiter")
          }
          if (!(params.SERVICE_NAME?.trim()) && !(params.TARGET_JOB?.trim())) {
            error("SERVICE_NAME or TARGET_JOB is required")
          }
          if (!env.JENKINS_URL?.trim()) {
            error("JENKINS_URL is not available in environment")
          }
        }
      }
    }

    stage('Resolve Job') {
      steps {
        script {
          def credId = (env.JENKINS_API_CREDENTIALS_ID ?: 'jenkins-api-user-token').trim()
          def root = (env.JENKINS_URL ?: '').trim().replaceAll('/+$', '')
          def baseCandidates = []
          baseCandidates << 'http://127.0.0.1:8080'
          baseCandidates << 'http://localhost:8080'
          if (root) baseCandidates << root
          baseCandidates = baseCandidates.unique()

          def apiBase = ''
          withCredentials([usernamePassword(credentialsId: credId, usernameVariable: 'API_USER', passwordVariable: 'API_TOKEN')]) {
            for (b in baseCandidates) {
              def ping = sh(
                returnStatus: true,
                script: """#!/bin/bash
                  curl -fsS --connect-timeout 4 --max-time 8 -u \"$API_USER:$API_TOKEN\" -o /dev/null \"${b}/api/json\"
                """
              )
              if (ping == 0) {
                apiBase = b
                break
              }
            }
          }
          if (!apiBase) {
            error("Could not reach Jenkins API from this agent using credential '${credId}'. Tried: ${baseCandidates.join(', ')}")
          }
          env.JENKINS_API_BASE = apiBase

          def explicitJob = (params.TARGET_JOB ?: '').trim()
          if (explicitJob) {
            env.RESOLVED_JOB = explicitJob
            echo "Resolved target job (explicit): ${env.RESOLVED_JOB} via ${env.JENKINS_API_BASE}"
            return
          }

          def svc = (params.SERVICE_NAME ?: '').trim().toLowerCase()
          def svcOriginal = svc // Keep original for exact match
          svc = svc.replaceAll(/[_\s]+/, '-')
          svc = svc.replaceAll(/-service$/, '')
          svc = svc.replaceAll(/-svc$/, '')

          // Check if service name indicates frontend
          def isFrontend = svcOriginal.contains('frontend') || svcOriginal.contains('react') || svcOriginal.contains('angular') || svcOriginal.contains('vue')

          // Build candidate list - order matters (most specific first)
          def candidates = []

          // Always try exact service-name matches first
          if (svcOriginal) {
            candidates << "${svcOriginal}"
            candidates << "dev-backend-${svcOriginal}"
            candidates << "dev-frontend-${svcOriginal}"
          }

          // Frontend patterns
          if (isFrontend) {
            candidates << "dev-frontend-${svcOriginal}"
            candidates << "dev-frontend-${svc}"
            candidates << "${svcOriginal}"
            candidates << "${svc}"
          }

          // Backend patterns
          candidates << "dev-backend-${svc}-service"
          candidates << "dev-backend-${svc}"
          candidates << "dev-backend-${svc}-svc"
          candidates << "dev-backend-${svcOriginal}"

          // Generic patterns (try exact name)
          candidates << "${svc}-service"
          candidates << "${svc}"
          candidates << "${svcOriginal}"

          // Remove duplicates while preserving order
          candidates = candidates.unique()

          def found = ''
          withCredentials([usernamePassword(credentialsId: credId, usernameVariable: 'API_USER', passwordVariable: 'API_TOKEN')]) {
            for (c in candidates) {
              def parts = c.split('/') as List
              def jobPath = parts.collect { seg -> "job/${seg}" }.join('/')
              def status = sh(
                returnStatus: true,
                script: """#!/bin/bash
                  curl -fsS --connect-timeout 4 --max-time 8 -u \"$API_USER:$API_TOKEN\" -o /dev/null \"${env.JENKINS_API_BASE}/${jobPath}/api/json\"
                """
              )
              if (status == 0) {
                found = c
                break
              }
            }
          }

          // If candidates didn't match, search Jenkins for matching jobs
          if (!found) {
            echo "Static candidates failed: ${candidates.join(', ')}"
            echo "Searching Jenkins API for jobs matching '${svc}'..."

            withCredentials([usernamePassword(credentialsId: credId, usernameVariable: 'API_USER', passwordVariable: 'API_TOKEN')]) {
              // Fetch all jobs from Jenkins (recursive search)
              def jobsJson = sh(
                returnStdout: true,
                script: """#!/bin/bash
                  curl -g -sS --connect-timeout 8 --max-time 30 -u \"$API_USER:$API_TOKEN\" \\
                    \"${env.JENKINS_API_BASE}/api/json?tree=jobs[name,jobs[name,jobs[name]]]\" 2>/dev/null || echo '{}'
                """
              ).trim()

              def jobsData = readJSON text: (jobsJson ?: '{}')
              def allJobs = []

              // Recursively collect all job names
              def collectJobs
              collectJobs = { items, prefix = '' ->
                items.each { job ->
                  if (job instanceof Map && job.name) {
                    def fullName = prefix ? "${prefix}/${job.name}" : job.name
                    allJobs << fullName
                    if (job.jobs) {
                      collectJobs(job.jobs, fullName)
                    }
                  }
                }
              }
              collectJobs(jobsData.jobs ?: [])

              echo "Found ${allJobs.size()} jobs in Jenkins"

              // Search for jobs containing the service name
              def svcNormalized = svc.replaceAll('-', '')
              def matchingJobs = allJobs.findAll { jobName ->
                def jobNormalized = jobName.toLowerCase().replaceAll('-', '')
                jobNormalized.contains(svcNormalized) || jobNormalized.contains(svc)
              }

              if (matchingJobs) {
                echo "Found matching jobs: ${matchingJobs.take(10).join(', ')}"
                // Prefer jobs with 'dev-backend' or 'dev-frontend' prefix
                def preferred = matchingJobs.find { it.toLowerCase().startsWith('dev-') }
                found = preferred ?: matchingJobs[0]
                echo "Selected job: ${found}"
              }
            }
          }

          if (!found) {
            echo "Tried static candidates: ${candidates.join(', ')}"
            echo "Also searched Jenkins API but found no matching jobs for '${svc}'"
            error("Could not resolve target job for service '${params.SERVICE_NAME}'. Pass TARGET_JOB explicitly.")
          }

          env.RESOLVED_JOB = found
          echo "Resolved target job (preferred): ${env.RESOLVED_JOB} via ${env.JENKINS_API_BASE}"
        }
      }
    }

    stage('Trigger') {
      steps {
        script {
          def credId = (env.JENKINS_API_CREDENTIALS_ID ?: 'jenkins-api-user-token').trim()
          def targetEnv = params.TARGET_ENV.toLowerCase()
          def jobParts = env.RESOLVED_JOB.split('/') as List
          def jobPath = jobParts.collect { seg -> "job/${seg}" }.join('/')

          def payload = ''
          withCredentials([usernamePassword(credentialsId: credId, usernameVariable: 'API_USER', passwordVariable: 'API_TOKEN')]) {
            payload = sh(
              returnStdout: true,
              script: """#!/bin/bash
                set -e
                curl -g -sS --connect-timeout 4 --max-time 12 -u \"$API_USER:$API_TOKEN\" \"${env.JENKINS_API_BASE}/${jobPath}/lastSuccessfulBuild/api/json?tree=number,actions[parameters[name,value]]\"
              """
            ).trim()
          }

          if (!payload) {
            error("Could not fetch lastSuccessfulBuild payload for ${env.RESOLVED_JOB}")
          }

          def data = readJSON text: payload
          if (!(data instanceof Map) || !data.number) {
            error("No last successful build found for ${env.RESOLVED_JOB}")
          }

          def paramMap = [:]
          def actions = data.actions instanceof List ? data.actions : []
          actions.each { a ->
            def rows = (a instanceof Map && a.parameters instanceof List) ? a.parameters : []
            rows.each { p ->
              if (p instanceof Map && p.name) {
                paramMap[p.name.toString()] = p.value
              }
            }
          }

          if (!paramMap || paramMap.isEmpty()) {
            error("Last successful build #${data.number} has no parameters for ${env.RESOLVED_JOB}")
          }

          // Force namespace/env across all common parameter keys so stale
          // values from last successful build (e.g. earth) never leak.
          def forcedEnvKeys = [
            'ENVIRONMENT', 'ENV', 'TARGET_ENV', 'TARGET_ENVIRONMENT',
            'NAMESPACE', 'TARGET_NAMESPACE',
            'environment', 'env', 'target_env', 'target_environment',
            'namespace', 'target_namespace'
          ]
          forcedEnvKeys.each { k ->
            paramMap[k] = targetEnv
          }

          // Also override any existing env-like key name pattern.
          (paramMap.keySet() as List).each { key ->
            def keyStr = key.toString()
            if (keyStr ==~ /(?i).*(^|_|-)(env|environment|namespace)(_|-|$).*/ ) {
              paramMap[keyStr] = targetEnv
            }
          }

          def paramValues = []
          paramMap.each { k, v ->
            if (v instanceof Boolean) {
              paramValues << booleanParam(name: k.toString(), value: (Boolean) v)
            } else {
              paramValues << string(name: k.toString(), value: (v == null ? '' : v.toString()))
            }
          }

          echo "Loaded params from last successful build: #${data.number}"
          echo "Final forced env namespace=${targetEnv}"
          echo "Param count=${paramValues.size()}"

          if (params.DRY_RUN) {
            echo "[DRY_RUN] Would trigger: ${env.RESOLVED_JOB}"
            paramMap.each { k, v ->
              echo "[DRY_RUN] ${k}=${v == null ? '' : v.toString()}"
            }
            return
          }

          def child = build(
            job: env.RESOLVED_JOB,
            parameters: paramValues,
            wait: false,
            propagate: false
          )
          echo "Triggered target job: ${env.RESOLVED_JOB} | Queue/Build ref: ${child}"
        }
      }
    }
  }
}
