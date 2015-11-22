Vagrant.configure(2) do |config|
  config.vm.box = "puppetlabs/debian-7.8-32-nocm"

  config.vm.provision "shell" do |script|
    script.path = "scripts/deploy.sh"
    script.privileged = false
    script.keep_color = true
  end
end
