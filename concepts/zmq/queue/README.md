ZMQ Queue Example

* c1: upstream component
* c2: queue component (load balancing)
* C3: downstream component

Run n instances of upstream components in separate terminals, like this:
```
  ./c1.py <name> <delay>
```
where name is used to identify the component (so that we can later see what
downstream component gto work from which upstream component), and delay is the
time in seconds (float) between each work item being produced.

Run 1 instance of the queue component (no arguments).
```
  ./c2.py
```
Run m instances of downstream components with:
```
  ./c3.py <delay>
```
where 'delay' is the time the component will to work on an item, ie. the time
between two work item pickups.

The startup can happen in any order.  Once running, any component can be shut
down and restarted.  You can start multiples of `c1.py` and `c3.py`.

observe that:
  - items remain ordered
  - downstream components get fair shares
  - upstream components are never blocked 
    (no send buffer fills up until we fill available memory)
  - the code footprint is very manageable

