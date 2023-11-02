#!/bin/zsh


mkdir logs
mkdir tv-data
mkdir tv-data/low-tf-data
mkdir tv-data/base-data
read -p "Please Enter Dropbox Path: E.g. /Users/user/Dropbox:" -r dropbox
ln -sf "$dropbox"/Trader/secret .
ln -sf "$dropbox"/Trader/traderv3/logs/archive logs/archive



