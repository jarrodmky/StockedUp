import typing
import math
from pandas import DataFrame

from kivy.lang import Builder
from kivy.properties import BooleanProperty, ObjectProperty, NumericProperty, StringProperty
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.button import Button
from kivy.uix.floatlayout import FloatLayout
from kivy.uix.label import Label
from kivy.uix.recycleview import RecycleView

from debug import debug_message

# adapted from https://stackoverflow.com/questions/44463773/kivy-recycleview-recyclegridlayout-scrollable-label-problems#comment75948118_44463773
# and from https://github.com/jefpadfi/PandasDataframeGUIKivy/blob/master/pdfkivygui/dfguik.py

Builder.load_file("dataframetable.kv")

class TableHeaderCell(Button) :
    pass

class TableHeader(BoxLayout) :

    header_cell_parent = ObjectProperty(None)

    def populate(self, column_names : typing.List[str], columns_relative_size : typing.List[float]) -> None :
        assert len(column_names) == len(columns_relative_size)
        for idx, column_name in enumerate(column_names) :
            relative_size_hint = (columns_relative_size[idx], None)
            self.header_cell_parent.add_widget(TableHeaderCell(text=column_name))

class TableDataCell(Label) :

    text = StringProperty(None)
    is_even = BooleanProperty(None)
    odd_colour = [0.2, 0.2, 0.2, 1]
    even_colour = [0.25, 0.25, 0.25, 1]

class TableData(RecycleView) :

    def populate(self, dataframe : DataFrame) :

        self.data = []
        for i, column_values in dataframe.to_dict(orient="index").items() :
            is_even = i % 2 == 0
            for text in column_values.values() :
                self.data.append({"text" : str(text), "is_even" : is_even})

class Table(BoxLayout) :

    table_header = ObjectProperty(None)
    table_data = ObjectProperty(None)

    nrows = NumericProperty(None)
    ncols = NumericProperty(None)

    def __init__(self, dataframe, column_relative_sizes, **kwargs) :
        super(Table, self).__init__(**kwargs)
        
        self.nrows = len(dataframe.index)
        self.ncols = len(dataframe.columns)

        self.table_header.populate(dataframe.columns, column_relative_sizes)
        self.table_data.populate(dataframe)

class DataFrameTable(FloatLayout) :

    def __init__(self, **kwargs) :
        super(DataFrameTable, self).__init__(**kwargs)

        self.sorting_by_column = None
        self.ascending = True
        
        self.query_expression = ""

    def set_data_frame(self, dataframe : DataFrame, column_relative_sizes : typing.List[float]) :
        sum = 0.0
        for size in column_relative_sizes :
            sum += size
        assert math.isclose(sum, 1.0), "Column sizes should add to 1"

        self.dataframe = dataframe
        self.relative_sizes = column_relative_sizes
        self.sort_by(self.dataframe.columns[0])

    def sort_by(self, sort_by_column : str) :
        assert sort_by_column in self.dataframe.columns
        if sort_by_column != self.sorting_by_column :
            self.sorting_by_column = sort_by_column
            self.ascending = True
        else :
            self.ascending = not self.ascending
        self.__refresh()

    def filter_by(self, expression : str) :
        self.query_expression = expression
        self.__refresh()

    def __refresh(self) :
        display_df = self.dataframe.sort_values(self.sorting_by_column, ascending=self.ascending)
        if self.query_expression != "" :
            try :
                display_df.query(self.query_expression, inplace=True)
            except Exception as e :
                debug_message(f"[DataFrameTable] Query failed! {e}")
        self.clear_widgets()
        self.add_widget(Table(display_df, self.relative_sizes))
