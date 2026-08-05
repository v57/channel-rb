# frozen_string_literal: true

require 'oj'

module ChannelRB
  module JSONCodec
    module_function

    def dump(object)
      Oj.dump(object, mode: :compat)
    end

    def load(string)
      Oj.load(string, mode: :strict)
    end
  end
end
