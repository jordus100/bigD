from PIL import Image
import numpy as np
import json
#import matplotlib.pyplot as plt

img = Image.open("raw_image_data.png")

img = img.convert("RGB")

colors = [
    (25, 0, 117),
    (93, 26, 201),
    (176, 0, 150),
    (247, 25, 66),
    (247, 82, 82),
    (237, 149, 104),
    (255, 212, 105),
    (255, 255, 145),
    (255, 250, 181),
    (255, 255, 235),
    (255, 255, 255)
]

populacja_waw = 1765000

left = 20.852142
right = 21.257111
top = 52.366347
bottom = 52.100750

color_values = list(range(10, -1, -1))

color_to_value = {color: value for color, value in zip(colors, color_values)}

def find_closest_color_and_value(pixel):
    pixel_array = np.array(pixel)
    color_array = np.array(colors)
    distances = np.sqrt(np.sum((color_array - pixel_array) ** 2, axis=1))
    closest_color_index = np.argmin(distances)
    closest_color = colors[closest_color_index]
    value = color_to_value[closest_color]
    return value

width, height = img.size
image_values = np.zeros((height, width), dtype=int)
for y in range(height):
    for x in range(width):
        pixel = img.getpixel((x, y))
        value = find_closest_color_and_value(pixel)
        image_values[y, x] = value
        
#np.set_printoptions(threshold=np.inf)
#print(image_values)

image_values = image_values[~np.all(image_values == 0, axis=1)]
image_values = image_values[:, ~np.all(image_values == 0, axis=0)]

#plt.imshow(image_values, cmap='gray')
#plt.axis('off')
#plt.show()

height, width = image_values.shape

lr = round( (right - left) / width , 6)
tb = round( (top - bottom) / height , 6)

#print(lr)
#print(tb)

support = []

y = top
for row in image_values:
    x = left
    for pixel in row:
        support.append([pixel, round(x,6), round(y,6)])
        x += lr
    y -= tb
    
#print(support)

count = 0
for record in support:
    count += record[0]

ppl_per_point = round(populacja_waw / count, 0)

#print(ppl_per_point)

for record in support:
    record[0] = record[0] * ppl_per_point
#print(support)
    
final = []
for record in support:
    if(record[0] != 0):
        final.append(record)
        
data = [{
    "coordinates": {
        "lon": record[1],
        "lat": record[2]
    },
    "population": record[0]
} for record in final]

#print(data)

with open('population_density.json', 'w') as json_file:
    json.dump(data, json_file, indent=2)