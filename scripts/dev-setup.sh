#!/bin/zsh


mkdir logs
read -p "Please Enter Dropbox Path: E.g. /Users/user/Dropbox" -r dropbox
ln -sf "$dropbox"/Trader .
