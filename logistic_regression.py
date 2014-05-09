import numpy as np
import pickle, os
import random

NUM_HEROES = 110
NUM_FEATURES = NUM_HEROES * 2

class D2LogisticRegression:
    def __init__(self, model_root='logistic_regression'):
        model_path = os.path.join(model_root, 'model.pkl')
        with open(model_path, 'rb') as input_file:
            self.model = pickle.load(input_file)

    def transform(self, my_team, their_team):
        X = np.zeros(NUM_FEATURES, dtype=np.int8)
        for hero_id in my_team:
            X[hero_id - 1] = 1
        for hero_id in their_team:
            X[hero_id - 1 + NUM_HEROES] = 1
        return X
        
        #def recommend(self, my_team, their_team, hero_candidates):
        #    '''Returns a list of (hero, probablility of winning with hero added) recommended to complete my_team.'''
        #    team_possibilities = [(candidate, my_team + [candidate]) for candidate in hero_candidates]

        #    prob_candidate_pairs = []
        #    for candidate, team in team_possibilities:
        #        query = self.transform(team, their_team)
        #        prob = self.score(query) #self.model.predict_proba(query)[0][1]
        #        prob_candidate_pairs.append((prob, candidate))
            #prob_candidate_pairs = sorted(prob_candidate_pairs, reverse=True)[0:5 - len(my_team)]
        #    prob_candidate_pairs = sorted(prob_candidate_pairs, reverse=True)[0:10]
        #    return prob_candidate_pairs
        
    def recommend(self, my_team, their_team, hero_candidates):
        '''Returns a list of (hero, probablility of winning with hero added) recommended to complete my_team.'''
        #team_possibilities = [(candidate, my_team + [candidate]) for candidate in hero_candidates]
        total = len(my_team) + len(their_team)
        remain = 10 - total
        numMy = 5 - len(my_team)
        numtheir = 5 - len(their_team)
        prob_table = {}
        for i in range(150):
            candidates = random.sample(hero_candidates, remain)
            m_team = my_team + candidates[0:numMy]
            t_team = their_team + candidates[numMy:]
            query = self.transform(m_team, t_team)
            prob = self.score(query)
            for role in candidates[0:numMy]:
                if role in prob_table:
                    prob_table[role] = max(prob, prob_table[role])
                else:
                    prob_table[role] = prob

        prob_candidate_pairs = []
        for role in prob_table:
            prob_candidate_pairs.append((prob_table[role], role))

        prob_candidate_pairs = sorted(prob_candidate_pairs, reverse=True)[0:10]
        return prob_candidate_pairs

    def score(self, query):
        '''Score the query using the model, considering both radiant and dire teams.'''
        radiant_query = query
        dire_query = np.concatenate((radiant_query[NUM_HEROES:NUM_FEATURES], radiant_query[0:NUM_HEROES]))
        rad_prob = self.model.predict_proba(radiant_query)[0][1]
        dire_prob = self.model.predict_proba(dire_query)[0][0]
        return (rad_prob + dire_prob) / 2

    def predict(self, dream_team, their_team):
        '''Returns the probability of the dream_team winning against their_team.'''
        dream_team_query = self.transform(dream_team, their_team)
        return self.score(dream_team_query)
        #return self.model.predict_proba(dream_team_query)[0][1]
