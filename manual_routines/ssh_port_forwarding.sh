#ssh port forwarding setup for the TNG AWS PostgreSQL server
##Run this before connecting to the database
## CHANGE TO YOUR SETTINGS, ESPECIALLY THE SSH USER (second part of the command)

#Linux version
# -f Go to background
# -L local connection - Specifies that connections to the given TCP port or Unix socket on the local (client) host are to be forwarded to the given host and port, or Unix socket, on the remote side.
# -N Do not execute a remote command, for forwarding port only
ssh -f -L 65432:postgresql.cluster-cjghupwohy3q.eu-west-2.rds.amazonaws.com:5432 user2@vpn.bredcap.org.uk -N


#Windows version
#seemingly working
ssh -fN -L 65432:postgresql.cluster-cjghupwohy3q.eu-west-2.rds.amazonaws.com:5432 user2@vpn.bredcap.org.uk



#The -g option:
#https://unix.stackexchange.com/questions/10428/simple-way-to-create-a-tunnel-from-one-local-port-to-another
