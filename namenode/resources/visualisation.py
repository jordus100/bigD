import time
left = 20.852142
right = 21.257111
top = 52.366347
bottom = 52.100750
cell_size = 0.001

distances = spark.read.json("hdfs:///score.json")
collected_distances = distances.collect()
score_map = {(row['longitude'], row['latitude']): row['score'] for row in collected_distances}

width = int((right - left) / cell_size)
height = int((top - bottom) / cell_size)

image = np.zeros((height, width), dtype=float)

for (lon, lat), score in score_map.items():
    x = int((lon - left) / cell_size)
    y = int((top - lat) / cell_size)
    if 0 <= x < width and 0 <= y < height:
        image[y, x] = score

plt.figure(figsize=(10, (top - bottom) / (right - left) * 20))
plt.imshow(image, extent=(left, right, bottom, top), aspect=1.7, cmap='coolwarm')
cbar = plt.colorbar(label='Score')
cbar.set_ticks(np.linspace(np.min(image), np.max(image), num=10))
cbar.ax.set_yticklabels([f'{int(t)}' for t in cbar.get_ticks()])

plt.xlabel('Longitude')
plt.ylabel('Latitude')
plt.title('Sum of 1/distance to stops Map')
plt.axis('on')
plt.savefig(f'visualisation-{time.strftime("%Y-%m-%d-%H-%M-%S")}')
plt.show()