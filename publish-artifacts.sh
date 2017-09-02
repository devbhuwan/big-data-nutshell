#!/usr/bin/env bash
# oss.sonatype.org
echo -n oss.sonatype.org Username:
read -e username
echo -n oss.sonatype.org Password:
read -s password
echo

./gradlew uploadArchives\
        -PossrhUsername=${username}\
        -PossrhPassword=${password}\
        -PnexusUsername=${username}\
        -PnexusPassword=${password}

./gradlew closeAndReleaseRepository\
        -PossrhUsername=${username}\
        -PossrhPassword=${password}\
        -PnexusUsername=${username}\
        -PnexusPassword=${password}