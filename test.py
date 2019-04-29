import tensorflow as tf
import logging
import os
import errno
# Settings
flags = tf.app.flags
FLAGS = flags.FLAGS

#core params..
flags.DEFINE_string('model', 'gcn', 'model name')
flags.DEFINE_float('learning_rate', 0.01, 'initial learning rate')
flags.DEFINE_string("model_size", "small", "define model size")
flags.DEFINE_integer('graph', 1, ' training graph. must be specified.')


def main(argv=None):

    print("Flags Set")
    print(FLAGS.learning_rate)
    print(FLAGS.model)
    print(FLAGS.graph)
    file = open("copy.txt", "w")
    file.write(str(FLAGS.learning_rate))
    file.close()

if __name__ == '__main__':
    log = logging.getLogger('tensorflow')
    log.setLevel(logging.DEBUG)

    # create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # create file handler which logs even debug messages
    fh = logging.FileHandler('tensorflow.log')
    fh.setLevel(logging.ERROR)
    fh.setFormatter(formatter)
    log.addHandler(fh)
    # create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(logging.ERROR)
    ch.setFormatter(formatter)
    log.addHandler(ch)
    main()
    #  tf.app.run()
