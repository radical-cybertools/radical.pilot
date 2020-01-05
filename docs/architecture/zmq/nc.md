
  - port mode

    - remote.host.net:
      - nc -l 5000
          
    - local
      - proxy tunnel:a
        - ssh -D 5001 remote.host.net
      - proxy wrapper / forward:
        - mkfifo backpipe
        - nc -X 5 -x localhost:5001 localhost 5000 <&backpipe \
          | nc -l 5003 1>backpipe
      - appliction
        - nc localhost 5003

            
  - socket mode

    - remote.host.net:
      - nc -l 5000
          
    - local
      - proxy tunnel:a
        - ssh -D /tmp/ssh.sock remote.host.net
      - proxy wrapper / forward:
        - mkfifo backpipe
        - nc -X 5 -x -U /tmp/ssh.sock localhost 5000 <&backpipe \
          | nc -l 5003 1>backpipe
      - appliction
        - nc localhost 5003
