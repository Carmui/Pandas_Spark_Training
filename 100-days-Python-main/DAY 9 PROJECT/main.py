from replit import clear
from art import logo

print(logo)

bid_dict = {}
is_finished = False


while is_finished is not True:
    name = input("Name: ")
    bid = input("Bid: ")
    bid_dict[name] = bid
    ask = input("Are there any other users who want to bid? (yes or no) ")

    if ask == "yes":
        clear()
    else:
        is_finished = True


for key, value in bid_dict.items():
    highest_offer = 0
    winner = "Error"
    if int(value) > highest_offer:
        highest_offer = value
        winner = key
      
print(f"The winner is: {winner} with the {highest_offer} bid.")