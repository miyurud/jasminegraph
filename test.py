import tensorflow as tf
import sys
import logging
import os
import errno
# Settings
flags = tf.app.flags
FLAGS = flags.FLAGS
flags.DEFINE_string('train_prefix', '/var', 'name of the object file that stores the training data. must be specified.')
flags.DEFINE_string('train_worker', '14' , 'specify the worker')
flags.DEFINE_string('graph_id', '0' , 'specify the graphID')
#core params..
flags.DEFINE_string('model', 'gcn', 'model name')
flags.DEFINE_float('learning_rate', 0.01, 'initial learning rate')
flags.DEFINE_string("model_size", "small", "define model size")



def main(argv=None):

    # for i in range(len(argv)):
    #     if argv[i]=="--train_prefix":
    #         train_prefix = argv[i+1]
    #     if argv[i]=="--graph_id":
    #         graph_id = argv[i+1]
    # file = open("train_prefix.txt", "w")
    # file.write(train_prefix)
    # file.write(graph_id)
    print("Flags Set")
    print(FLAGS.learning_rate)
    print(FLAGS.model)
    file = open("copy.txt", "w")
    file.write(FLAGS.train_prefix)
    file.write(FLAGS.graph_id)
    file.write(FLAGS.train_worker)
    file.write(FLAGS.model)
    file.write(str(FLAGS.learning_rate))
    file.close()

if __name__ == '__main__':
    # log = logging.getLogger('tensorflow')
    # log.setLevel(logging.DEBUG)
    #
    # # create formatter and add it to the handlers
    # formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    #
    # # create file handler which logs even debug messages
    # fh = logging.FileHandler('tensorflow.log')
    # fh.setLevel(logging.ERROR)
    # fh.setFormatter(formatter)
    # log.addHandler(fh)
    # # create console handler with a higher log level
    # ch = logging.StreamHandler()
    # ch.setLevel(logging.ERROR)
    # ch.setFormatter(formatter)
    # log.addHandler(ch)

    file = open("before.txt", "w")
    file.write("before")
    main()
    #  tf.app.run()
