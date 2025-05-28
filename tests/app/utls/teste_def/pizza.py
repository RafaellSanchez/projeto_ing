def make_pizza(size, *toppings):
    print(f'\n making a {size}, pizza with the following toppings: ')
    for topping in toppings:
        print(f'- {topping}')
