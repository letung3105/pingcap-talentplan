#!/bin/bash

cargo watch -s 'cargo doc && http target/doc --port 8080'
