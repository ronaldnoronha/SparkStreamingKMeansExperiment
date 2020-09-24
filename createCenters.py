from sklearn.datasets import make_blobs

def create_data(n_samples, n_features, centers, std):
    features, target = make_blobs(n_samples = n_samples,
                                  # two feature variables,
                                  n_features = n_features,
                                  # four clusters,
                                  centers = centers,
                                  # with .65 cluster standard deviation,
                                  cluster_std = std,
                                  # shuffled,
                                  shuffle = True)
    return features, target

if __name__ == "__main__":
    centers, centersNum = create_data(8,3,8,3)
    with open('centers.txt','w') as f:
        for i in centers:
            f.write(' '.join([str(j) for j in i])+'\n')
