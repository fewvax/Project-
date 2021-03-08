# You can add methods to this class if you wish.
# The only thing you cannot do is change the names
# of the member variables.
# The argument could be made that Hero and Object should
# be the same class, but I did not do this because:
#   - In my own solution, I have some additional, different
#     methods in each of these two classes.
#   - I thought it might introduce some unnecessary confusion.
class Hero:
    def __init__(self, symbol, start_x, start_y):
        self.symbol = symbol
        self.x = start_x
        self.y = start_y
        self.old_x = start_x
        self.old_y = start_y

    # You don't have to use this if you don't want to.
    def draw_on_board(self, board):
        if self.x != self.old_x or self.y != self.old_y:
            board[self.y][self.x] = self.symbol
            board[self.old_y][self.old_x] = ' '
            self.old_x = self.x
            self.old_y = self.y
        else:
            board[self.y][self.x] = self.symbol

# You can add methods to this class if you wish.
# The only thing you cannot do is change the names
# of the member variables.
class Object:
    def __init__(self, symbol, x, y):
        self.symbol = symbol
        self.x = x
        self.y = y

    # You don't have to use this if you don't want to.
    def draw_on_board(self, board):
        board[self.y][self.x] = self.symbol

class Building:
    def __init__(self, x, y):
        self.width = 6
        self.height = 4
        self.door_height = 2
        self.door_width = 2
        self.x = x
        self.y = y

    def draw_on_board(self, board):
        for i in range(self.x,self.x+self.width):
            board[self.y][i] = '-'
            board[self.y+self.height-1][i] = '-'
        for j in range(self.y+1,self.y+self.height-1):
            board[j][self.x] = '|'
            board[j][self.x+self.width-1] = '|'
        door_x = self.x + 2
        door_y = self.y+self.height-1
        for ii in range(door_x,door_x+self.door_width):
            for jj in range(door_y-1,door_y+self.door_height-1):
                board[jj][ii] = '&' 



    # Returns True if given the location of this building, the point
    # indicated by (x,y) touches this building in any way.
    def contains(self, x, y):
        if x in range(self.x,self.x+self.width):
            x_condition = True
        else:
            x_condition = False
        if y in range(self.y,self.y+self.height):
            y_condition = True
        else:
            y_condition = False
        result = x_condition and y_condition
        return result



# You don't need to use this function, but I found it useful
# when I wanted to inspect the board from the Interpreter /
# after running the program.
def print_board(board):
    for row in board:
        for spot in row:
            print(spot, end='')
        print()

class Game:
    def __init__(self, input_file_name):
        # You can add member variables to this class.
        # However, you cannot change the names of any of
        # the member variables below; the autograder will
        # expect these member variables to have the names
        # that they have.
        self.width = 0
        self.height = 0
        self.hero = None
        self.num_objects = 0
        self.board = None
        self.buildings = []
        self.objects = []
        self.down_key = None
        self.up_key = None
        self.right_key = None
        self.left_key = None
        self.read_input_file(input_file_name)
        self.hero.draw_on_board(self.board)
        self.user_quit = False

    def read_input_file(self, input_file_name):
        f = open(input_file_name,'r')
        data = f.readlines()
        self.width = int(data[0].split()[0])
        self.height = int(data[0].split()[1])
        self.board = [[' '] * self.width for i in range(self.height)]
        for i in range(1,self.width-1):
            self.board[0][i] = '-'
            self.board[self.height-1][i] = '-'
        for j in range(1,self.height-1):
            self.board[j][0] = '|'
            self.board[j][self.width-1] = '|'
        hero_info = data[1]
        self.hero = Hero(hero_info.split()[0],int(hero_info.split()[1]),int(hero_info.split()[2]))
        self.up_key = data[2].split()[0]
        self.left_key = data[3].split()[0]
        self.down_key = data[4].split()[0]
        self.right_key = data[5].split()[0]
        for i in range(6,len(data)):
            if 'o' in data[i]:
                self.num_objects += 1
                obj = Object(data[i].split()[1],int(data[i].split()[2]),int(data[i].split()[3]))
                obj.draw_on_board(self.board)
                self.objects.append(obj)
            if 'b' in data[i]:
                build = Building(int(data[i].split()[1]),int(data[i].split()[2]))
                build.draw_on_board(self.board)
                self.buildings.append(build)




    def print_game(self):
        for row in self.board:
            for spot in row:
                print(spot, end='')
            print()

    def game_ended(self):
        return self.all_objects_collected() or self.user_quit

    def all_objects_collected(self):
        return self.num_objects == 0
    
    def collide_with_b(self):
        result = False
        for build in self.buildings:
            if build.contains(self.hero.x,self.hero.y):
                result = True
        if self.hero.x <= 0 or self.hero.y <= 0:
            result = True
        if self.hero.x >= self.width-1 or self.hero.y >= self.height-1:
            result = True
        return result

    def collect_object(self):
        for obj in self.objects:
            if obj.x == self.hero.x and obj.y == self.hero.y:
                self.num_objects -= 1
                obj.x = -1
                obj.y = -1
              
                
    def run(self):
        # Finish the implementation. Implement movement
        # and collision detection. You have much flexibity in
        # how you modify this function. For example, you can
        # remove the definitions and usages of the game_ended()
        # and all_objects_collected() methods if you don't want
        # to use those. (My own implementation uses those methods.)
        quit_cmds = ['q', 'end', 'exit']
        moving_cmds = [self.down_key,self.up_key,self.left_key,self.right_key]
        while not self.game_ended():
            self.print_game()
            inp = input("Enter: ")
            if inp in quit_cmds:
                self.user_quit = True
            else:
                if inp == self.right_key:
                    self.hero.x += 1
                    if not self.collide_with_b():
                        self.hero.draw_on_board(self.board)
                    else:
                        self.hero.x -= 1
    
                if inp == self.left_key:
                    self.hero.x -= 1
                    if not self.collide_with_b():
                        self.hero.draw_on_board(self.board)
                    else:
                        self.hero.x += 1
                            
                if inp == self.up_key:
                    self.hero.y -= 1
                    if not self.collide_with_b():
                        self.hero.draw_on_board(self.board)
                    else:
                        self.hero.y += 1

                if inp == self.down_key:
                    self.hero.y += 1
                    if not self.collide_with_b():
                        self.hero.draw_on_board(self.board)
                    else:
                        self.hero.y -= 1
                if inp not in moving_cmds:
                    print("Invalid command")
                self.collect_object()
        self.print_game()
        if self.user_quit:
            print("You are a quitter!")
        else:
            print("Congratulations: you've collected all of the items!")
g = Game("input1.txt")
g.run()
            

# You can have lines like these when you run the program,
# but make sure that they are commented out or removed
# when you submit this file to the autograder.
