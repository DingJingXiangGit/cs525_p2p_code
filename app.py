import tornado.ioloop
import tornado.web
from pprint import pprint
from engine import Engine
from k_nearest_neighbors import D2KNearestNeighbors, my_distance, poly_weights_recommend, poly_weights_evaluate
from logistic_regression import D2LogisticRegression
import json

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("Hello, world")

class QueryHandler(tornado.web.RequestHandler):
    def get(self):
        params = self.get_argument("params")
        param_json = json.loads(params)
        pprint(param_json);
        user_team = param_json["user_team"]
        opponent_team = param_json['opponent_team']
        user_force = param_json["user_force"]
        opponent_force = param_json["opponent_force"]

        prob_recommendation_pairs = engine.recommend(user_team, opponent_team)
        recommendations = [hero for prob, hero in prob_recommendation_pairs]
        #prob = engine.predict(user_team, opponent_team)

        result = {"recommendations": recommendations}
        self.write(json.dumps(result));

    def post(self):
        params = self.get_argument("params")
        param_json = json.loads(params)
        pprint(param_json);
        user_team = param_json["user_team"]
        opponent_team = param_json['opponent_team']
        user_force = param_json["user_force"]
        opponent_force = param_json["opponent_force"]

        prob_recommendation_pairs = engine.recommend(user_team, opponent_team)
        recommendations = [hero for prob, hero in prob_recommendation_pairs]
        #prob = engine.predict(user_team, opponent_team)

        result = {"recommendations": recommendations}


class RegisterHandler(tornado.web.RequestHandler):
    def post(self):

engine = Engine(D2LogisticRegression())
#engine = Engine(D2KNearestNeighbors())

application = tornado.web.Application([
    (r"/", MainHandler),
    (r"/query", QueryHandler),
    (r"/post", RegisterHandler),
])

if __name__ == "__main__":
    application.listen(8888)
    tornado.ioloop.IOLoop.instance().start()
