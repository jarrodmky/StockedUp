import typing
import math
from polars import DataFrame

from kivy.lang import Builder
from kivy.metrics import mm
from kivy.properties import ObjectProperty, NumericProperty
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.button import Button
from kivy.uix.floatlayout import FloatLayout
from kivy.uix.recycleview import RecycleView

from PyJMy.debug import debug_message

# adapted from https://stackoverflow.com/questions/44463773/kivy-recycleview-recyclegridlayout-scrollable-label-problems#comment75948118_44463773
# and from https://github.com/jefpadfi/PandasDataframeGUIKivy/blob/master/pdfkivygui/dfguik.py

class TableHeaderCell(Button) :
    pass

class TableHeader(BoxLayout) :

    def populate(self, column_names : typing.List[str], columns_relative_size : typing.List[float]) -> None :
        assert len(column_names) == len(columns_relative_size)
        for idx, column_name in enumerate(column_names) :
            relative_size = columns_relative_size[idx]
            self.add_widget(TableHeaderCell(text=column_name, size_hint_x=relative_size))

class TableData(RecycleView) :

    table_grid_layout = ObjectProperty(None)

    def populate(self, row_dictionaries : typing.List[typing.Dict[str, typing.Any]], columns_relative_size : typing.List[float]) -> None :
        self.table_grid_layout.bind(minimum_height = self.table_grid_layout.setter("height"))

        columns_sizes = {}
        for idx, relative_size in enumerate(columns_relative_size) :
            columns_sizes[idx] = relative_size * self.parent.width
        self.table_grid_layout.cols_minimum = columns_sizes
        
        self.data = []
        for i, row in enumerate(row_dictionaries) :
            for j, (_, value) in enumerate(row.items()) :
                self.data.append({
                    "text" : str(value),
                    "background_color" : [0.4, 0.4, 0.4, 1] if (i % 2 == 0) else [0.25, 0.25, 0.25, 1],
                    "size_hint_x" : columns_relative_size[j],
                    "height" : mm(8)})

class Table(BoxLayout) :

    table_header = ObjectProperty(None)
    table_data = ObjectProperty(None)

    nrows = NumericProperty(None)
    ncols = NumericProperty(None)

    def __init__(self, dataframe, column_relative_sizes, **kwargs) :
        super(Table, self).__init__(**kwargs)
        
        self.nrows = len(dataframe)
        self.ncols = len(dataframe.columns)

        self.table_header.populate(dataframe.columns, column_relative_sizes)
        self.table_data.populate(dataframe.to_dicts(), column_relative_sizes)

DataFrameTransform = typing.Callable[[DataFrame], DataFrame]

class DataFrameTable(FloatLayout) :

    def __init__(self, **kwargs) :
        super(DataFrameTable, self).__init__(**kwargs)

        self.sorting_by_column = None
        self.ascending = True
        
        self.query_expression : DataFrameTransform = lambda df : df

    def set_data_frame(self, dataframe : DataFrame, column_relative_sizes : typing.List[float]) -> None :
        sum = 0.0
        for size in column_relative_sizes :
            sum += size
        assert math.isclose(sum, 1.0), "Column sizes should add to 1"

        self.dataframe = dataframe
        self.relative_sizes = column_relative_sizes
        self.sort_by(self.dataframe.columns[0])

    def sort_by(self, sort_by_column : str) -> None :
        assert sort_by_column in self.dataframe.columns
        if sort_by_column != self.sorting_by_column :
            self.sorting_by_column = sort_by_column
            self.descending = False
        else :
            self.descending = not self.descending
        self.__refresh()

    def filter_by(self, expression : DataFrameTransform) -> None :
        self.query_expression = expression
        self.__refresh()

    def __refresh(self) :
        display_df = self.dataframe.sort(self.sorting_by_column, descending=self.descending)
        try :
            display_df = self.query_expression(display_df)
        except Exception as e :
            debug_message(f"[DataFrameTable] Query failed! {e}")
        self.clear_widgets()
        self.add_widget(Table(display_df, self.relative_sizes))
