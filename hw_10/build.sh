#!/bin/bash
echo "Building generator image ..."
docker build -t hw10_generator ./generator
echo "Generator image built. Let's go?"