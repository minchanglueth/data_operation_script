class Person:  # base class or parent class
    def __init__(self, name):  # constructor
        self.name = name

    def get_name(self):
        return self.name


class Employee(Person):  # derived class or subclass
    def is_employee(self):
        return True


person = Person("Pythonista")  # object creation/instantiation
print(person.name)

employee = Employee("Employee Pythonista")
print(employee.name)
# print(employee.is_employee())