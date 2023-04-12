### scratch architecture ideas
* control node
  * maintains clone of messages from both active and backup nodes, swaps
    * over if a node dies
  * atexit.register(dump_state())
* listener node
  * local and remote versions -- the local version uses a bound socket,
    the remote version probably has to run an HTTP server
  * data source definitions as factory functions
  * then also interpretation rules
    * logging on for waked nodes, off for slept nodes
  * polling interval sometimes
* handler node
  * similarly, local and remote versions
  * default execution type?
    * python options include
      * the dynamic code construction thing
      * directly importing the module
      * insisting we can call everything from the system interpreter
        * (i.e., we wrap everything with fire or argparse or whatever)
  * monitoring definitions, also factory functions
    * watch process stdout/stderr?
    * watch a particular logfile?
    * something else
    * this can have criteria like:
      * if we didn't see an exception message and the file is in the directory, great