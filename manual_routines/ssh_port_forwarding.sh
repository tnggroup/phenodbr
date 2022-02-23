#ssh port forwarding setup for the TNG AWS PostgreSQL server
#Run this before connecting to the database
# -f Go to background
# -L local connection - Specifies that connections to the given TCP port or Unix socket on the local (client) host are to be forwarded to the given host and port, or Unix socket, on the remote side.
# -N Do not execute a remote command, forforwarding port only
#ssh -f vpn(dot)bredcap(dot)org(dot)uk -L 65432: vpn.bredcap.org.uk:5432 -N
ssh -f -L 65432:postgresql.cluster-cjghupwohy3q.eu-west-2.rds.amazonaws.com:5432 user2@vpn.bredcap.org.uk -N