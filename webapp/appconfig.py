import yaml

class AppConfig:

    #------------------
    # Standard Functions
    #------------------
    def __init__(self, config_file):

        self.config_file = config_file
        with open(self.config_file, "r") as ymlfile:
            self.cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)

        self.depth = 0
        self.str_out = ""

    def __str__(self):
        out_info = '-----------------------\nAppConfig Object'
        out_info += '\nConfig file used: ' + self.config_file + '\n'
        out_info += self.list_full_config()
        return out_info

    def __txt__(self):
        return self.__str__


    #------------------
    # Public Functions 
    #------------------
    def get_full_config(self):

        return self.cfg


    def get_config_section(self, path_to_section):

        out = f'Section <{path_to_section}> not found'

        if isinstance(path_to_section, list):
            tmp_dict = self.cfg
            try:
                for path_step in path_to_section:
                    tmp_dict = tmp_dict[path_step]
                return tmp_dict
            except(KeyError, TypeError):
                return out
        else:
            for section_name in self.cfg:
                if section_name == path_to_section:
                    out = self.cfg[section_name]
                    break
            return out


    def get_config_value(self, key_sought):

        ret_values = []

        for section_name in self.cfg:
            section_obj = self.cfg[section_name]
            bread_crumb = "//" + section_name

            self._process_section(key_sought, section_obj, bread_crumb, ret_values)

        if len(ret_values) == 0:
            out = f'Config key <{key_sought}> not found'
        else:
            out = ret_values

        return out


    def list_full_config(self):
        
        self.str_out = 'The full config values:\n-----------------------'
    
        for section_name in self.cfg:
            section_obj = self.cfg[section_name]

            self.str_out += f'\n{section_name} (is of type: {type(section_obj)}):'
            self._print_section(section_obj, self.depth)

        return self.str_out


    #------------------
    # Internal Functions
    #------------------
    def _process_section(self, key_sought, section_in, bread_crumb, ret_values):

        if isinstance(section_in, dict):
            # handle dict
            for key in section_in:
                bread_crumb = bread_crumb + "/" + key
                if isinstance(section_in[key], dict):
                    self._process_section(key_sought, section_in[key], bread_crumb, ret_values)
                else:
                    if key == key_sought:
                        ret_values.append([section_in[key], key, bread_crumb])

        elif isinstance(section_in, list):
            #handle list
            pass
        else:
            print(f'WARNING: unhandled object type {type(section_in)}')


    def _print_section(self, section_in, level):
        level += 1
        spacing = ' '*3*level

        if isinstance(section_in, dict):
            # handle dict
            for key in section_in:
                if isinstance(section_in[key], dict):
                    self.str_out += f'\n{spacing}{key}'
                    self._print_section(section_in[key], level)
                elif isinstance(section_in[key], list):
                    self.str_out += f'\n{spacing}{key}'
                    self.print_list(section_in[key], spacing + '   ')
                else:
                    self.str_out += f'\n{spacing}{key} -> {section_in[key]}'
        elif isinstance(section_in, list):
            #handle list
            self.print_list(section_in, spacing)
        else:
            self.str_out += f'\nWARNING: unhandled object type {type(section_in)}'


    def print_list(self, list_in, spacing):
        for item in list_in:
            self.str_out += f'\n{spacing}> {item}'



#-------------------------------------------------------------------------------
# The script entry point
#-------------------------------------------------------------------------------

if __name__ == "__main__":

    # define the config file
    config_file_test = "<the file>"

    # the the config object
    config = AppConfig(config_file_test)
    config_settings = config.get_full_config()

    # A few tests...
    #print(config)

    #print(config.get_config_section('environments'))

    #print(config.get_config_section('gugus'))


    #print(config_settings["environments"]["prod"]["Schedule"])

    #config.list_full_config()

