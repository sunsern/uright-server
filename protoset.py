import numpy as np

from uright.prototype import PrototypeDTW

class ProtosetDTW(object):
    """A set of prototypes of the same label"""
    def __init__(self, label, min_cluster_size=3, alpha=0.5,
                 center_type='centroid', state_reduction=False):
        self.trained_prototypes = []
        self.label = label
        self.min_cluster_size = min_cluster_size
        self.alpha = alpha
        self.center_type = center_type
        self.state_reduction = state_reduction

    def train(self, weighted_ink_groups, verbose=True):
        self.trained_prototypes = []
        for weighted_ink_list in weighted_ink_groups:
            # we skip small clusters 
            if len(weighted_ink_list) > self.min_cluster_size:
                ink_data, weights = zip(*weighted_ink_list)
                proto = PrototypeDTW(self.label, alpha=self.alpha)
                avg_score = proto.train(
                    ink_data, 
                    obs_weights=weights,
                    center_type=self.center_type,
                    state_reduction=self.state_reduction)
                self.trained_prototypes.append(proto)

    def toJSON(self):
        json_obj = {}
        json_obj['type'] = 'DTW'
        json_obj['label'] = self.label
        json_obj['prototypes'] = [p.toJSON() 
                                  for p in self.trained_prototypes]
        return json_obj
