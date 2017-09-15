#!/bin/bash
#
# ./build.sh [instance] [version] [workspace] [python distribution]
#
# Build script for the project to produce a packaged distribution.
#
# Example: ./build.sh rusrep 1.0.0 /home/rusrep /usr/share/python/2.7.11
#
# Requirements: virtualenv and pip must be installed with the targeted python distribution.

APPLICATION_NAME="django"


instance=$1
if [ -z "$instance" ] ; then
    instance="rusrep"
    echo "Setting default instance to ${instance}"
fi

version=$2
if [ -z "$version" ] ; then
    version="1.0.0"
    echo "Setting default version to ${version} "
fi

workspace=$3
if [ -z "$workspace" ] ; then
    workspace="/home/${instance}"
    echo "Setting default workspace to ${workspace}"
fi
builddir="${workspace}/${APPLICATION_NAME}"

p=$4
if [ -z "$p" ] ; then
    p=$(which python)
    echo "Setting default python to system ${p}"
fi


revision=$(git rev-parse HEAD)
echo "instance=${instance}"
echo "version=${version}"
echo "workspace=${workspace}"
echo "builddir=${builddir}"
echo "python distribution=${p}"
echo "revision=${revision}"



# Remove any previous builds
if [ -d target ] ; then
    rm -rf target
fi


# Create a workspace
if [ -d $builddir ] ; then
    rm -rf $builddir
fi
rsync -av --progress --exclude='build.sh' --exclude='.git' . $builddir


# Setup and activate a virtual environment
$p/bin/virtualenv $builddir/virtualenv
. $builddir/virtualenv/bin/activate


# Retrieve the dependencies for python 2.7
pip install -r $builddir/requirements_py2.7.txt --cache-dir=$workspace


# Create the artifact
mkdir -p target
package=target/$instance-$version.tar.gz
tar -pczf $package -C $workspace $APPLICATION_NAME


# clean up
deactivate
rm -rf $builddir

tar tf $package
rc=$?
if [ $rc -eq 0 ] ; then
    echo "I think we are done for today."
    exit 0
else
    echo -e "Unable to create the artifact."
    exit 1
fi

exit 1