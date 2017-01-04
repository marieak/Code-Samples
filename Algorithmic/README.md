maze_solver.py
---------------
An interview style prompt/response challenge taken from Checkio.org (code challenges)


Objective
--------------------
The maze has no walls, but bushes surround the path on each side. 

If a players move into a bush, they lose. 

The maze is presented as a matrix (a list of lists) where 1 is a bush and 0 is part of the path. 
    
The maze's size is 12 x 12 and the outer cells are also bushes. 

Players start at cell (1,1). The exit is at cell (10,10). 

You need to find a route through the maze. 

Players can move in only four directions:

    - South (down [1,0]) 
    
    - North (up [-1,0])
    
    - East (right [0,1])
    
    - West (left [0, -1])
    

The route is described as a string consisting of different characters: 

    "S"=South, "N"=North, "E"=East, and "W"=West.


Reason for including
--------------------
Demonstrates an ability to find clear, concise and simple solutions to algorithmically challenging problems

Knowlege of basic language features in cluding:
  - Data structures (set, dictionary, tuple, list)
  - Recursion
  - Dictionary & tuple unpacking
  - Python 2 & 3  compatibility
