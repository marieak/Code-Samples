#!/bin/bash
# Download OpenVPN configuration files from vendor (IPVanish in this case)

# Clean out the config folder
rm -r ovpn_configs/*

# Download config files
wget -O- http://www.ipvanish.com/software/configs/ | grep -oP '"ipv.*-US-.*\.ovpn"' | tr -d '"' | perl -ne 'print "https://www.ipvanish.com/software/configs/$_"' | xargs wget -P ovpn_configs 

# Update the ovpn.txt file containing paths to openvpn configurations
for i in ovpn_configs/*.ovpn; do
    sed -i '12s/.*/auth-user-pass auth.txt/' $i
done


