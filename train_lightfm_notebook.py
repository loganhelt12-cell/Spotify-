
"""LightFM training notebook script (runnable as a script or in a notebook).
This uses synthetic data to demonstrate training, saving embeddings, and building a Faiss index.
Run: python train_lightfm_notebook.py
Produces: /mnt/data/models/lightfm_item_embeddings.npy and faiss index file.
"""
import numpy as np
import scipy.sparse as sp
from lightfm import LightFM
import faiss, os, json
os.makedirs('/mnt/data/models', exist_ok=True)

# Synthetic dataset
n_users = 2000
n_items = 5000
rng = np.random.RandomState(42)

# simulate sparse implicit interaction matrix (users x items)
rows = rng.randint(0, n_users, size=20000)
cols = rng.randint(0, n_items, size=20000)
data = rng.randint(1, 5, size=20000)  # counts/weights
X = sp.coo_matrix((data, (rows, cols)), shape=(n_users, n_items)).tocsr()

# train-test split (simple)
train = X.copy()
# remove some interactions for 'test'
test = sp.csr_matrix((n_users, n_items))

# instantiate model
model = LightFM(no_components=64, loss='warp', learning_rate=0.05)
print('Training LightFM...')
model.fit(train, epochs=10, num_threads=4)

item_embeddings = model.item_embeddings.astype('float32')  # shape (n_items, dim)
np.save('/mnt/data/models/lightfm_item_embeddings.npy', item_embeddings)
print('Saved item embeddings to /mnt/data/models/lightfm_item_embeddings.npy')

# build Faiss index (Flat L2 for demo)
d = item_embeddings.shape[1]
index = faiss.IndexFlatL2(d)
index.add(item_embeddings)
faiss.write_index(index, '/mnt/data/models/faiss_index.bin')
print('Wrote Faiss index to /mnt/data/models/faiss_index.bin')

# sample recommend: get top-10 nearest neighbors for a random item
import heapq
sample_item = rng.randint(0, n_items)
D, I = index.search(item_embeddings[sample_item:sample_item+1], 11)
print('Neighbors for item', sample_item, '->', I[0][:10].tolist())

# Save metadata (basic)
meta = {'n_users': n_users, 'n_items': n_items, 'dim': d}
with open('/mnt/data/models/meta.json', 'w') as f:
    json.dump(meta, f)
print('Training artifact creation complete.')
