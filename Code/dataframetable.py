import typing
from pandas import DataFrame

from kivy.lang import Builder
from kivy.properties import BooleanProperty, ObjectProperty, NumericProperty, StringProperty
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.button import Button
from kivy.uix.label import Label
from kivy.uix.recycleview import RecycleView
from kivy.uix.scrollview import ScrollView

from debug import debug_message

# adapted from https://stackoverflow.com/questions/44463773/kivy-recycleview-recyclegridlayout-scrollable-label-problems#comment75948118_44463773
# and from https://github.com/jefpadfi/PandasDataframeGUIKivy/blob/master/pdfkivygui/dfguik.py

Builder.load_file("dataframetable.kv")

class TableHeaderCell(Button) :
    pass

class TableHeader(ScrollView) :

    header_cell_parent = ObjectProperty(None)

    def populate(self, column_names : typing.List[str]) -> None :
        for column_name in column_names :
            self.header_cell_parent.add_widget(TableHeaderCell(text=column_name))

class TableDataCell(Label) :

    text = StringProperty(None)
    is_even = BooleanProperty(None)
    odd_colour = [0.2, 0.2, 0.2, 1]
    even_colour = [0.25, 0.25, 0.25, 1]

class TableData(RecycleView) :

    nrows = NumericProperty(None)
    ncols = NumericProperty(None)

    def populate(self, dataframe : DataFrame) :
        
        self.nrows = len(dataframe.index)
        self.ncols = len(dataframe.columns)

        self.data = []
        for i, column_values in dataframe.to_dict(orient="index").items() :
            is_even = i % 2 == 0
            for text in column_values.values() :
                self.data.append({"text" : str(text), "is_even" : is_even})

class Table(BoxLayout) :

    table_header = ObjectProperty(None)
    table_data = ObjectProperty(None)

    def __init__(self, dataframe, *args, **kwargs) :
        super(Table, self).__init__(*args, **kwargs)

        self.table_header.populate(dataframe.columns)
        self.table_data.populate(dataframe)
        self.table_data.fbind('scroll_x', self.scroll_with_header)

    def scroll_with_header(self, obj, value) :
        self.table_header.scroll_x = value

class DataFrameTable(BoxLayout) :

    def __init__(self, **kwargs) :
        super(DataFrameTable, self).__init__(**kwargs)

        self.sorting_by_column = None
        self.ascending = True
        
        self.query_expression = ""

    def set_data_frame(self, dataframe : DataFrame) :
        self.dataframe = dataframe
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
        self.add_widget(Table(display_df))
