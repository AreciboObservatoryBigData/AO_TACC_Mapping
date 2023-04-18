
# main menu option
def get_option_main(options):
    print("Please select an option:")
    for i in range(len(options)):
        print(str(i) + " - " + options[i])
    option = input("Option: ")
    option = int(option)
    return option

