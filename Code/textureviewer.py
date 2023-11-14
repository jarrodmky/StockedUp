import typing

from array import array as CArray

from kivy.graphics.texture import Texture
from kivy.graphics import Rectangle
from kivy.uix.boxlayout import BoxLayout

class TextureViewer(BoxLayout) :

    def __init__(self, **kwargs) :
        super(TextureViewer, self).__init__(**kwargs)
        self.orientation = "vertical"

    def set_texture(self, buffer_data : bytes, buffer_size : typing.Tuple[int, int]) -> None :
        texture = Texture.create(size=buffer_size, colorfmt='rgba')

        (buffer_width, buffer_height) = buffer_size
        buffer_byte_width = buffer_width * 4
        buffer_array = CArray("B", buffer_data)
        flipped_height_buffer = CArray("B")
        for upper_row in range(0, buffer_height) :
            lower_row = buffer_height - upper_row - 1
            lower_row_view = buffer_array[lower_row * buffer_byte_width : (lower_row + 1) * buffer_byte_width]

            flipped_height_buffer.frombytes(lower_row_view.tobytes())
        texture.blit_buffer(flipped_height_buffer, colorfmt='rgba', bufferfmt='ubyte')

        with self.canvas :
            self.rect = Rectangle(texture=texture, pos=self.pos, size=buffer_size)