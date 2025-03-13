import lazy_object_proxy

class ExpensiveObject:
    def __init__(self):
        print("ExpensiveObject is being initialized...")

    def compute(self, x, y):
        print(f"Computing {x} + {y}")
        return x + y

# Create a lazy proxy for the compute method
lazy_compute = lazy_object_proxy.Proxy(lambda: ExpensiveObject().compute)

# The ExpensiveObject is not initialized yet
print("Lazy proxy created, but ExpensiveObject is not initialized yet.")

# Accessing the compute method will trigger the initialization of ExpensiveObject
result = lazy_compute(3, 4)
print(f"The result is: {result}")

# The ExpensiveObject is now initialized
print("ExpensiveObject has been initialized.")