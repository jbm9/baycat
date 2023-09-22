#!/bin/bash

set -e

# A script to quickly set up the local dev environment

# To override the defaults below, simply call this script with them in
# the environment.

: ${PYTHON='python'}  # Python interpreter to use
: ${VENV_DIR='.venv'} # Location we're setting up within
: ${DEBUG=}          # Whether or not to log copiously to stdout

if [ "x${VIRTUAL_ENV}" != "x" ];
then
    echo "We already have an active virtual env, exiting."
    exit 1
fi

[ $DEBUG ] && echo "Setting up Virtual env in ${VENV_DIR}... $(date)"
${PYTHON} -m venv ${VENV_DIR}

[ $DEBUG ] && echo "Virtual env initalized, activating it now $(date)"
source ${VENV_DIR}/bin/activate

[ $DEBUG ] && echo "Virtual env active.  Installing baycat for development. $(date)"
pip install --editable .[test]

[ $DEBUG ] && echo "Done. $(date)"
exit 0  # Needed to keep make happy.
