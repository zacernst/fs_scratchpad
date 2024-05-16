from __future__ import annotations

from typing import List, Optional


def get_feature_class_by_name(feature_name: str) -> type:
    for feature_class in Feature.__subclasses__():
        if feature_class.name == feature_name:
            return feature_class


class classproperty:
    """
    Plagiarized from https://stackoverflow.com/questions/76249636/class-properties-in-python-3-11
    """

    def __init__(self, func):
        self.fget = func

    def __get__(self, instance, owner):
        return self.fget(owner)


class DataSource:
    '''
    This is a class that represents a data source. It could be a CSV, a database, or anything else.
    '''
    name: str


class CSVDataSource(DataSource):
    '''
    This is a subclass of `DataSource` that represents a CSV file.
    '''
    path: str
    dialect: Optional[str] = None


class Feature:
    name: str
    data_sources: Optional[List[DataSource]] = []
    entity: type

    @classproperty
    def dependency_names(cls) -> List[str]:
        return [
            name for name in cls.calculate.__annotations__.keys() if name != "return"
        ]

    @classproperty
    def dependency_classes(cls) -> List[type]:
        return [
            globals()[class_name]
            for class_name_string, class_name in cls.calculate.__annotations__.items()
            if class_name_string != "return"
        ]


class Entity:
    name = "entity"

    def __init__(self):
        self.value_cache = {}

    def __repr__(self):
        output = f"{self.name}("
        for feature_class in self.feature_list:
            output += f"{feature_class.name}={self.calculate_feature_value(feature_class)}, "
        output = output[:-2] + ")"
        return output

    def get_table(self):
        pass

    def calculate_feature_value(self, feature_class: type):
        '''
        This is a recursive function that calculates the value of a feature
        by calculating the values of its dependencies.
        '''
        if feature_class in self.value_cache:
            value = self.value_cache[feature_class]
        else:
            for dependency_class in feature_class.dependency_classes:
                dependency_value = self.calculate_feature_value(dependency_class)
                self.value_cache[dependency_class] = dependency_value
            arguments = [
                self.value_cache[dependency_class]
                for dependency_class in feature_class.dependency_classes
            ]
            value = feature_class.calculate(self, *arguments)
            self.value_cache[feature_class] = value
        return value

    @classmethod
    def get_features(cls):
        feature_list = []
        for feature_class in Feature.__subclasses__():
            if feature_class.entity is cls:
                feature_list.append(feature_class)
        return feature_list

    @classproperty
    def feature_list(cls):
        return cls.get_features()

#### A data scientist or MLE would define the following classes in a separate file ####
#### They wouldn't need to know about the classes defined above beyond just importing them. ####
class Rectangle(Entity):
    '''
    A `Rectangle` is an `Entity` that has a `width` and a `length`, which would be
    defined somewhere like a CSV file. Then there's another `Feature` called `area`
    that calculates the area of the rectangle based on the width and length.
    '''
    name = "rectangle"


class Width(Feature):
    '''
    This would come from a CSV or somewhere like that, but for now we'll just hardcode it.
    '''
    entity: type = Rectangle
    name: str = "width"

    def calculate(self) -> float:
        return 3.0


class Length(Feature):
    '''
    This would come from a CSV or somewhere like that, but for now we'll just hardcode it.
    '''
    entity: type = Rectangle
    name: str = "length"
    
    def calculate(self) -> float:
        return 2.0


class Area(Feature):
    '''
    Features introspect their own `calculate` method to determine their dependencies.
    So the DS or MLE doesn't ever explicitly call `calculate` on a `Feature`. It gets
    called automatically when the `Entity` is accessed.
    '''
    entity: type = Rectangle
    name: str = "area"

    def calculate(self, width: Width, length: Length) -> float:
        return width * length


if __name__ == "__main__":
    '''
    We create a new `Rectangle` object and print it. This will trigger the calculations.
    '''
    bob = Rectangle()
    print(bob)