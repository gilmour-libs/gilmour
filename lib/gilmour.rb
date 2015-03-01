# encoding: utf-8

# This is required to check whether Mash class already exists
def class_exists?(class_name)
      klass = Module.const_get(class_name)
      return klass.is_a?(Class)
rescue NameError
      return false
end

require_relative 'gilmour/base'
