from __future__ import annotations

from abc import ABC, abstractmethod
import copy
import csv
import functools
import hashlib
from typing import Any, Dict, Generator, List, Optional
import uuid

import rich

NO_VALUE = "__NO_VALUE__"


def get_feature_class_by_name(feature_name: str) -> type:
    for feature_class in Feature.__subclasses__():
        if feature_class.name == feature_name:
            return feature_class


def global_classes(cls: type) -> Generator:
    for _, obj in iterate_globals():
        if isinstance(obj, type) and issubclass(obj, cls) and obj is not cls:
            yield obj


def global_instances(cls: type) -> Generator:
    for _, obj in iterate_globals():
        if isinstance(obj, cls):
            yield obj


def iterate_globals():
    globals_keys = copy.copy(
        list(globals().keys())
    )  # So we can iterate over the keys without changing the dict
    for globals_key in globals_keys:
        obj = globals()[globals_key]
        yield globals_key, obj


class FeatureValueCache:
    def __init__(self):
        self._cache = {}

    def __getitem__(self, key: Any) -> Any:
        return self._cache.get(key, NO_VALUE)

    def __setitem__(self, key: Any, value: Any) -> Any:
        self._cache[key] = value

    def __delitem__(self, key: Any) -> None:
        del self._cache[key]

    def __contains__(self, key: Any) -> bool:
        return key in self._cache


class classproperty:
    """
    Plagiarized from https://stackoverflow.com/questions/76249636/class-properties-in-python-3-11
    """

    def __init__(self, func):
        self.fget = func

    def __get__(self, instance, owner):
        return self.fget(owner)


class DataSource(ABC):
    """
    This is a class that represents a data source. It could be a CSV, a database, or anything else.
    """

    name: str

    def __init__(self, name: str = ""):
        self.name = name
        self.entity_feature_mappings = []

    @abstractmethod
    def yield_data(self) -> Generator:
        """
        Every subclass of `DataSource` must implement this method.
        It should yield data from the source as a sequence of dictionaries.
        """
        pass

    def __call__(self):
        return self.yield_data()
    
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.name})"

    def add_entity_feature_mapping(
        self, feature: type, feature_key: str, name_key: str
    ) -> None:
        entity_feature_mapping = {
            "feature": feature,
            "feature_key": feature_key,
            "name_key": name_key,
        }
        self.entity_feature_mappings.append(entity_feature_mapping)

    def has_entity(self, entity: type) -> bool:
        return any(
            [
                entity_mapping["feature"].entity is entity
                for entity_mapping in self.entity_feature_mappings
            ]
        )
    
    def __iter__(self):
        return self.yield_data()
    
    def entity_name_keys(self, entity: type) -> List[str]:
        return set(
            entity_mapping["name_key"]
            for entity_mapping in self.entity_feature_mappings
            if entity_mapping["feature"].entity is entity
        )
    
    def entity_feature_mapping_for_name_key(self, name_key: str) -> Generator:
        for entity_feature_mapping in self.entity_feature_mappings:
            if entity_feature_mapping["name_key"] == name_key:
                yield entity_feature_mapping
    
    def __lt__(self, other):
        self.add_entity_feature_mapping(*other)


class CSVDataSource(DataSource):
    """
    This is a subclass of `DataSource` that represents a CSV file.
    """

    path: str
    dialect: Optional[str] = None

    def __init__(self, name: str, path: str, dialect: Optional[str] = None):
        self.name = name
        self.path = path
        self.dialect = dialect
        super().__init__(name)

    def yield_data(self) -> Generator:
        with open(self.path) as f:
            dict_reader = csv.DictReader(f, dialect=self.dialect)
            for row in dict_reader:
                yield row


class DataCatalog:
    """
    This is a class that represents a catalog of data sources.
    """

    data_sources: List[DataSource]

    def __init__(self):
        self.data_sources = []

    def add_data_source(self, data_source: DataSource):
        self.data_sources.append(data_source)

    def get_data_source(self, name: str) -> DataSource:
        for data_source in self.data_sources:
            if data_source.name == name:
                return data_source


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

    def raw_process(self, value: Any) -> Any:
        """Just a placeholder."""
        return value


class Entity:
    name = "entity"

    def __init__(self, session: Session = None, name: str = ""):
        self.value_cache = session.cache  #This will be set by the `Session` object
        self.session = session
        self.name = name

    def __repr__(self):
        output = f"{self.name}: ("
        for feature_class in self.feature_list:
            output += (
                f"{feature_class.name}={self.calculate_feature_value(feature_class)}, "
            )
        output = output[:-2] + ")"
        return output

    def get_table(self):
        pass

    def get_feature_hash(self, feature_class: type) -> str:
        unique = ''.join([self.__class__.__name__, feature_class.__name__, self.name])
        return hashlib.md5(unique.encode()).hexdigest()

    def calculate_feature_value(self, feature_class: type):
        """
        This is a recursive function that calculates the value of a feature
        by calculating the values of its dependencies.
        """
        # If the value is already in the cache, return it
        feature_hash = self.get_feature_hash(feature_class)
        if feature_hash in self.value_cache:
            value = self.value_cache[feature_hash]
        else:
            for dependency_class in feature_class.dependency_classes:
                dependency_value = self.calculate_feature_value(dependency_class)
                self.value_cache[self.get_feature_hash(dependency_class)] = dependency_value
            arguments = [
                self.value_cache[self.get_feature_hash(dependency_class)]
                for dependency_class in feature_class.dependency_classes
            ]
            value = feature_class.calculate(self, *arguments)
            self.value_cache[self.get_feature_hash(feature_class)] = value
        return value
    
    def stipulate_feature_value(self, feature_class: type, value: Any):
        self.value_cache[self.get_feature_hash(feature_class)] = feature_class.value_type(value)

    @classmethod
    def get_features(cls):
        feature_list = []
        for feature_class in Feature.__subclasses__():
            if feature_class.entity is cls:
                feature_list.append(feature_class)
        return feature_list

    @classmethod
    def get_data_sources(cls):
        data_sources = []
        for feature_class in cls.feature_list:
            for data_source in feature_class.data_sources:
                data_sources.append(data_source)
        return data_sources

    @classproperty
    def feature_list(cls):
        return cls.get_features()


data_sources = functools.partial(global_instances, DataSource)
features = functools.partial(global_classes, Feature)
entities = functools.partial(global_classes, Entity)


class Session:
    def __init__(self):
        self.data_sources = []
        self.entities = []
        self.features = []
        self.cache = FeatureValueCache()
        self.session_id = uuid.uuid4().hex
    
    def __enter__(self):
        self.populate()
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def get_data_source(self, name: str) -> DataSource:
        return self.data_catalog.get_data_source(name)
    
    def add_data_source(self, data_source: DataSource):
        self.data_sources.append(data_source)
    
    def add_entity(self, entity: Entity):
        self.entities.append(entity)
        entity.session = self
    
    def add_feature(self, feature: Feature):
        self.features.append(feature)
        feature.session = self

    def data_sources_with_entity(self, entity: type) -> Generator:
        for data_source in self.data_sources:
            if data_source.has_entity(entity):
                yield data_source

    def populate(self): 
        for data_source in data_sources():
            self.add_data_source(data_source)
        for feature in features():
            self.add_feature(feature)
        for entity in entities():
            self.add_entity(entity)
            entity.value_cache = self.cache
        
    def dump(self):
        for entity_type in self.entities:
            rich.print(f'Entity type: {entity_type.__name__}')
            for data_source in self.data_sources_with_entity(entity_type):
                entity_name_keys = data_source.entity_name_keys(entity_type)
                for row in data_source():
                    for entity_name_key in entity_name_keys:
                        entity_name = row[entity_name_key]
                        for entity_feature_mapping in data_source.entity_feature_mapping_for_name_key(
                            entity_name_key
                        ):
                            feature = entity_feature_mapping["feature"]
                            feature_key = entity_feature_mapping["feature_key"]
                            feature_value = row[feature_key]
                            entity_name = row[entity_name_key]
                            entity_cls = feature.entity
                            entity = entity_cls(session=self, name=entity_name)
                            entity.stipulate_feature_value(feature, feature_value)
                        rich.print(entity)

# Everything above this would be imported by the data scientist or MLE
# Everything below this is configuration
class Rectangle(Entity):
    """
    A `Rectangle` is an `Entity` that has a `width` and a `length`, which would be
    defined somewhere like a CSV file. Then there's another `Feature` called `area`
    that calculates the area of the rectangle based on the width and length.
    """

    name = "rectangle"


class Width(Feature):
    """
    This would come from a CSV or somewhere like that, but for now we'll just hardcode it.
    """

    entity: type = Rectangle
    name: str = "width"
    value_type: type = float

    def calculate(self) -> float:
        return 3.0


class Length(Feature):
    """
    This would come from a CSV or somewhere like that, but for now we'll just hardcode it.
    """

    entity: type = Rectangle
    name: str = "length"
    value_type: type = float

    def calculate(self) -> float:
        return 2.0


class Area(Feature):
    """
    Features introspect their own `calculate` method to determine their dependencies.
    So the DS or MLE doesn't ever explicitly call `calculate` on a `Feature`. It gets
    called automatically when the `Entity` is accessed.
    """

    entity: type = Rectangle
    name: str = "area"
    value_type: type = float

    def calculate(self, width: Width, length: Length) -> float:
        return width * length



if __name__ == "__main__":
    # First, we define a data source that has the width and length of a rectangle
    rectangle_size_data_source = CSVDataSource(
        name="rectangle_size", path="sample_data/rectangle_sizes.csv"
    )

    # Now we configure the data source by telling it which columns correspond to which features
    # Notice that the CSV has length and width, but no area.
    rectangle_size_data_source < (Width, "w_col", "rectangle_id")
    rectangle_size_data_source < (Length, "len_col", "rectangle_id")

    # Now we define a session and print all the Rectangle objects
    with Session() as session: 
        session.dump()