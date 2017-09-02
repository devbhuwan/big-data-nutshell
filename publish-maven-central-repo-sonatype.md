# How to publish a jar into Sonatype central repo

## Order of Operations

1. Create gpg key
2. Export gpg key
3. Publish gpg key to ubuntu server
4. Create account at Sonatype JIRA
5. Configure $HOME/.gradle/settings.gradle user and pass and keys settings
6. Configure gradle plugins
7. Release 

## Create gpg key

You can see the keys with gpg -K if you dont have a published key into Ubuntu Key Server please do:
```bash
gpg --gen-key
gpg -K
cd $HOME/.gnupg
gpg --export-secret-keys -o secring.gpg
gpg --send-keys --keyserver keyserver.ubuntu.com <KEY ID>
```

## If ERROR [gpg: agent_genkey failed: No such file or directory sKey generation failed: No such file or directory] 
```bash
pkill gpg-agent; strace -o /tmp/gpg-agent.trace gpg-agent --daemon
```
## Create Account in Sonatype JIRA

Now you need create an account an open a ticket to reserve a groupip. 

* https://issues.sonatype.org/secure/Signup!default.jspa
* https://issues.sonatype.org/secure/CreateIssue.jspa?issuetype=21&pid=10134

## Configure $HOME/.gradle/settings.gradle

edit Configure $HOME/.gradle/settings.gradle with the folloing content.
```bash
ossrhUsername=???
ossrhPassword=???
nexusUsername=???
nexusPassword=???

signing.keyId=??? [LSB 8-bit of FULL KEY IF The key ID must be in a valid form (eg 00B5050F or 0x00B5050F) given value: ABCDABCDABCDABCD]
signing.password=???
signing.secretKeyRingFile=...../secring.gpg
```
signing.keyId and signing.password you need get from USER TOKEN from NEXUS UI: https://oss.sonatype.org/#profile;User Token. You need login there using same user/pass you got from jira ticket. The property signing.secretKeyRingFil should be something like $HOME/.gnupg/secring.gpg

## Configure Gradle Plugins
build.gradle
```groovy
apply plugin: "eclipse"
apply plugin: "java"
apply plugin: "signing"
apply plugin: "maven"

group = "YOUR_GROUP_ID"
archivesBaseName = "YOUR_ARTIFACT_NAME"
version = "YOUR_VERSION"

sourceCompatibility = 1.8
targetCompatibility = 1.8

sourceSets {
    main.java.srcDirs = ["src/main/java"]
}

repositories {
  maven { url 'http://repo.spring.io/libs-milestone' }
  maven { url 'http://repo.spring.io/libs-release' }
  mavenCentral()
  maven { url "https://oss.sonatype.org/content/groups/public/" }
  mavenLocal()
}

dependencies {
	compile([

    ])
    testCompile([

    ])
}

eclipse {
    classpath {
       downloadSources=true
    }
}

task javadocJar(type: Jar) {
    classifier = 'javadoc'
    from javadoc
}

task sourcesJar(type: Jar) {
    classifier = 'sources'
    from sourceSets.main.allSource
}

artifacts {
    archives javadocJar, sourcesJar
}

buildscript {
    repositories {
        mavenCentral()
    }
    dependencies {
        classpath "io.codearte.gradle.nexus:gradle-nexus-staging-plugin:0.9.0"
    }
}

apply plugin: 'io.codearte.nexus-staging'

signing {
     sign configurations.archives
}

uploadArchives {
     repositories {
         mavenDeployer {
             beforeDeployment { MavenDeployment deployment -> signing.signPom(deployment) }

             repository(url: "https://oss.sonatype.org/service/local/staging/deploy/maven2/") {
                authentication(userName: ossrhUsername, password: ossrhPassword)
             }
             snapshotRepository(url: "https://oss.sonatype.org/content/repositories/snapshots/") {
                authentication(userName: ossrhUsername, password: ossrhPassword)
             }

             pom.project {
                 name project.name
                 description project.description
                 packaging 'jar'
                 url 'https://github.com/YOU/YOUR_REPO'
                 scm {
                     connection 'scm:git:https://github.com/YOU/YOUR_REPO.git'
                     developerConnection 'scm:git:git@github.com:YOU/YOUR_REPO.git'
                     url 'https://github.com/YOU/YOUR_REPO.git'
                 }
                 licenses {
                     license {
                         name 'The Unlicense'
                         url 'http://unlicense.org/'
                         distribution 'repo'
                     }
                 }
                 developers {
                     developer {
                         id = 'YOUR_ID_ANY_SRING'
                         name = 'YOUR_NAME'
                         email = 'YOUR_EMAIL'
                     }
                 }
             }
         }
     }
}

nexusStaging {
  username = "${nexusUsername}"
  password = "${nexusPassword}"
  packageGroup = "${group}"
}
```
## Release

```bash
./gradlew uploadArchives
./gradlew closeAndReleaseRepository
```

Goto: https://oss.sonatype.org/#staging-upload

## Resources

* http://central.sonatype.org/pages/gradle.html
* http://central.sonatype.org/pages/ossrh-guide.html
* http://packaging.ubuntu.com/pt-br/html/getting-set-up.html