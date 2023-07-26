import matplotlib.pyplot as plt
import numpy as np

############################################
# Plot size of relations with and without Yannakis
############################################
has_review_size_without_yannakis = 5159
has_review_size_with_yannakis = 5159

likes_size_without_yannakis = 23921
likes_size_with_yannakis = 13457

friend_of_size_without_yannakis = 39781
friend_of_size_with_yannakis = 39724

follows_size_without_yannakis = 91200
follows_size_with_yannakis = 36287

relations = ['hasReview', 'likes', 'friendOf', 'follows']
x = np.arange(len(relations))

fig = plt.figure()
ax = fig.add_subplot(111)
rects = ax.bar(x - 0.125, [has_review_size_with_yannakis, likes_size_with_yannakis, friend_of_size_with_yannakis, follows_size_with_yannakis], color='orange', width=0.25, label='with Yannakis')
ax.bar_label(rects, padding=3, size=10)
rects = ax.bar(x + 0.125, [has_review_size_without_yannakis, likes_size_without_yannakis, friend_of_size_without_yannakis, follows_size_without_yannakis], color='b', width=0.25, label='without Yannakis')
ax.bar_label(rects, padding=3, size=10)

ax.set_ylabel("Relation Size")
ax.set_xlabel("Relation")
ax.set_xticks(x, relations)
ax.legend(loc='upper left', ncols=2)
plt.show()

############################################
# Plot runtime of hash join and sort-merge join with and without Yannakis
############################################
runtime_hash_with_yannkis = 0.34
runtime_hash_without_yannkis = 0.90

runtime_sort_merge_with_yannkis = 0.30
runtime_sort_merge_without_yannkis = 0.48

algorithms = ['Hash Join', 'Sort Merge Join']
x = np.arange(len(algorithms))

fig = plt.figure()
ax = fig.add_subplot(111)
rects = ax.barh(x - 0.125, [runtime_hash_with_yannkis, runtime_sort_merge_with_yannkis], color='orange', height=0.25, label='with Yannakis')
ax.bar_label(rects, padding=3, size=10)
rects = ax.barh(x + 0.125, [runtime_hash_without_yannkis, runtime_sort_merge_without_yannkis], color='b', height=0.25, label='without Yannakis')
ax.bar_label(rects, padding=3, size=10)

ax.set_xlabel("Runtime (s)")
ax.set_ylabel("Join Algorithm")
ax.set_yticks(x, algorithms)
ax.legend(loc='upper right', ncols=2)
plt.show()
