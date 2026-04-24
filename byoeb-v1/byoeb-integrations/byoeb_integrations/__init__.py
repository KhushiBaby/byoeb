from byoeb_integrations.langfuse_bindings import register as register_langfuse_bindings

import os

current_dir = os.path.dirname(os.path.abspath(__file__))
test_environment_path = os.path.join(current_dir, 'test_keys.env')
test_environment_path = os.path.normpath(test_environment_path)


register_langfuse_bindings()