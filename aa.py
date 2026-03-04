import turtle

bob=turtle.Turtle()

def quadrato(t,lunghezza):
    for i in range(4):
        t.fd(lunghezza)
        t.lt(90)

quadrato(bob,100)